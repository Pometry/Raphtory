package com.raphtory.components.partition

import com.raphtory.components.querymanager._
import com.raphtory.algorithms._
import com.raphtory.algorithms.api.ClearChain
import com.raphtory.algorithms.api.Explode
import com.raphtory.algorithms.api.ExplodeSelect
import com.raphtory.algorithms.api.GlobalSelect
import com.raphtory.algorithms.api.GraphStateImplementation
import com.raphtory.algorithms.api.Iterate
import com.raphtory.algorithms.api.IterateWithGraph
import com.raphtory.algorithms.api.MultilayerView
import com.raphtory.algorithms.api.ReduceView
import com.raphtory.algorithms.api.Select
import com.raphtory.algorithms.api.SelectWithGraph
import com.raphtory.algorithms.api.Step
import com.raphtory.algorithms.api.StepWithGraph
import com.raphtory.algorithms.api.TableFilter
import com.raphtory.algorithms.api.WriteTo
import com.raphtory.components.Component
import com.raphtory.components.querymanager.CheckMessages
import com.raphtory.components.querymanager.CreatePerspective
import com.raphtory.components.querymanager.EndQuery
import com.raphtory.components.querymanager.ExecutorEstablished
import com.raphtory.components.querymanager.GraphFunctionComplete
import com.raphtory.components.querymanager.MetaDataSet
import com.raphtory.components.querymanager.PerspectiveEstablished
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.SetMetaData
import com.raphtory.components.querymanager.TableBuilt
import com.raphtory.components.querymanager.TableFunctionComplete
import com.raphtory.components.querymanager.VertexMessage
import com.raphtory.components.querymanager.VertexMessageBatch
import com.raphtory.config.PulsarConnector
import com.raphtory.config.TopicRepository
import com.raphtory.config.telemetry.StorageTelemetry
import com.raphtory.graph.GraphPartition
import com.raphtory.graph.LensInterface
import com.raphtory.output.PulsarOutputFormat
import com.raphtory.storage.pojograph.PojoGraphLens
import com.raphtory.storage.pojograph.messaging.VertexMessageHandler
import com.raphtory.time.Interval
import com.typesafe.config.Config
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api._

import java.util.concurrent.atomic.AtomicInteger

/** @DoNotDocument */
class QueryExecutor(
    partitionID: Int,
    storage: GraphPartition,
    jobID: String,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {

  var currentTimestamp: Long              = _
  var currentWindow: Option[Interval]     = _
  var graphLens: LensInterface            = _
  var sentMessageCount: AtomicInteger     = new AtomicInteger(0)
  var receivedMessageCount: AtomicInteger = new AtomicInteger(0)
  var votedToHalt: Boolean                = false
  var filtered: Boolean                   = false

  private val listeningTopics =
    if (totalPartitions > 1) Seq(topics.jobOperations(jobID), topics.vertexMessages(jobID))
    else Seq(topics.jobOperations(jobID))

  private val listener = topics.registerListener(
          s"query-executor-$jobID-$partitionID",
          handleMessage,
          listeningTopics,
          partitionID
  )

  private val taskManager = topics.jobStatus(jobID).endPoint

  private val neighbours =
    if (totalPartitions > 1)
      Some(topics.vertexMessages(jobID).endPoint)
    else
      None

  override def run(): Unit = {
    logger.debug(s"Job '$jobID' at Partition '$partitionID': Starting query executor consumer.")
    listener.start()
    taskManager sendAsync ExecutorEstablished(partitionID)
  }

  override def stop(): Unit = {
    listener.close()
    logger.debug(s"closing query executor consumer for $jobID on partition $partitionID")
    taskManager.close()
    neighbours match {
      case Some(neighbours) => neighbours.values.foreach(_.close())
      case None             =>
    }
  }

  override def handleMessage(msg: QueryManagement): Unit = {
    try {
      msg match {

        case VertexMessageBatch(msgBatch)                                     =>
          logger.trace(
                  s"Job '$jobID' at Partition '$partitionID': Executing 'VertexMessageBatch', '[${msgBatch
                    .mkString(",")}]'."
          )
          msgBatch.foreach(message => graphLens.receiveMessage(message))
          receivedMessageCount.addAndGet(msgBatch.size)

        case msg: GenericVertexMessage[_]                                     =>
          logger.trace(
                  s"Job '$jobID' at Partition '$partitionID': Executing 'VertexMessage', '$msg'."
          )
          graphLens.receiveMessage(msg)
          receivedMessageCount.addAndGet(1)

        case CreatePerspective(timestamp, window, actualStart, actualEnd)     =>
          val time = System.currentTimeMillis()
          val lens = PojoGraphLens(
                  jobID,
                  actualStart,
                  actualEnd,
                  superStep = 0,
                  storage,
                  conf,
                  neighbours,
                  sentMessageCount,
                  receivedMessageCount,
                  errorHandler
          )
          currentTimestamp = timestamp
          currentWindow = window
          graphLens = lens
          sentMessageCount.set(0)
          receivedMessageCount.set(0)
          taskManager sendAsync PerspectiveEstablished(lens.getSize())
          val id   = window match {
            case Some(value) =>
              s"${jobID}_partitionID_${partitionID}_time_${timestamp}_window_$value"
            case None        => s"${jobID}_partitionID_${partitionID}_time_$timestamp"
          }
          StorageTelemetry
            .pojoLensGraphSize(
                    id
            )
            .set(graphLens.getFullGraphSize)
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Created perspective at time '$timestamp' with window '$window'. in ${System
                    .currentTimeMillis() - time}ms"
          )

        case SetMetaData(vertices)                                            =>
          val time = System.currentTimeMillis()
          graphLens.setFullGraphSize(vertices)
          taskManager sendAsync MetaDataSet
          logger.debug(
                  s"Job $jobID at Partition '$partitionID': Meta Data set in ${System.currentTimeMillis() - time}ms"
          )

        case MultilayerView(interlayerEdgeBuilder)                            =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.explodeView(interlayerEdgeBuilder) {
            val sentMessages     = sentMessageCount.get()
            val receivedMessages = receivedMessageCount.get()
            graphLens.getMessageHandler().flushMessages().thenApply { _ =>
              taskManager sendAsync
                GraphFunctionComplete(partitionID, receivedMessages, sentMessages)

              logger
                .debug(s"Job '$jobID' at Partition '$partitionID': MultilayerView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages.")
            }
          }

        case ReduceView(defaultMergeStrategy, mergeStrategyMap, aggregate)    =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.reduceView(defaultMergeStrategy, mergeStrategyMap, aggregate) {
            val sentMessages     = sentMessageCount.get()
            val receivedMessages = receivedMessageCount.get()
            graphLens.getMessageHandler().flushMessages().thenApply { _ =>
              taskManager sendAsync
                GraphFunctionComplete(partitionID, receivedMessages, sentMessages)

              logger
                .debug(s"Job '$jobID' at Partition '$partitionID': MultilayerView function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages.")
            }
          }

        case Step(f)                                                          =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.runGraphFunction(f) {
            val sentMessages     = sentMessageCount.get()
            val receivedMessages = receivedMessageCount.get()
            graphLens.getMessageHandler().flushMessages().thenApply { _ =>
              taskManager sendAsync
                GraphFunctionComplete(partitionID, receivedMessages, sentMessages)

              logger
                .debug(s"Job '$jobID' at Partition '$partitionID': Step function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages.")
            }
          }

        case StepWithGraph(f, graphState)                                     =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.runGraphFunction(f, graphState) {

            val sentMessages     = sentMessageCount.get()
            val receivedMessages = receivedMessageCount.get()
            graphLens.getMessageHandler().flushMessages().thenApply { _ =>
              taskManager sendAsync
                GraphFunctionCompleteWithState(
                        partitionID,
                        receivedMessages,
                        sentMessages,
                        graphState = graphState
                )

              logger.debug(
                      s"Job '$jobID' at Partition '$partitionID': Step function on graph with accumulators finished in ${System
                        .currentTimeMillis() - time}ms and sent '$sentMessages' messages."
              )
            }
          }

        case Iterate(f, iterations, executeMessagedOnly)                      =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          val fun  =
            if (executeMessagedOnly)
              graphLens.runMessagedGraphFunction(f)(_)
            else
              graphLens.runGraphFunction(f)(_)
          fun {
            val sentMessages     = sentMessageCount.get()
            val receivedMessages = receivedMessageCount.get()
            graphLens.getMessageHandler().flushMessages().thenApply { _ =>
              taskManager sendAsync
                GraphFunctionComplete(
                        partitionID,
                        receivedMessages,
                        sentMessages,
                        graphLens.checkVotes()
                )

              votedToHalt = graphLens.checkVotes()
              logger.debug(
                      s"Job '$jobID' at Partition '$partitionID': Iterate function completed in ${System
                        .currentTimeMillis() - time}ms and sent '$sentMessages' messages with `executeMessageOnly` flag set to $executeMessagedOnly."
              )
            }
          }

        case IterateWithGraph(f, iterations, executeMessagedOnly, graphState) =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          val fun  =
            if (executeMessagedOnly)
              graphLens.runMessagedGraphFunction(f, graphState)(_)
            else
              graphLens.runGraphFunction(f, graphState)(_)
          fun {
            val sentMessages     = sentMessageCount.get()
            val receivedMessages = receivedMessageCount.get()
            votedToHalt = graphLens.checkVotes()
            graphLens.getMessageHandler().flushMessages().thenApply { _ =>
              taskManager sendAsync
                GraphFunctionCompleteWithState(
                        partitionID,
                        receivedMessages,
                        sentMessages,
                        votedToHalt,
                        graphState
                )

              logger.debug(
                      s"Job '$jobID' at Partition '$partitionID': Iterate function on graph with accumulators completed  in ${System
                        .currentTimeMillis() - time}ms and sent '$sentMessages' messages with `executeMessageOnly` flag set to $executeMessagedOnly."
              )
            }
          }

        case ClearChain()                                                     =>
          val time = System.currentTimeMillis()
          graphLens.clearMessages()
          taskManager sendAsync GraphFunctionComplete(partitionID, 0, 0)
          logger.debug(
                  s"Job $jobID at Partition '$partitionID': Messages cleared on graph in ${System
                    .currentTimeMillis() - time}ms."
          )

        case Select(f)                                                        =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.executeSelect(f) {
            taskManager sendAsync TableBuilt
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Select executed on graph in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case SelectWithGraph(f, graphState)                                   =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.executeSelect(f, graphState) {
            taskManager sendAsync TableBuilt
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Select executed on graph with accumulators in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case GlobalSelect(f, graphState)                                      =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          if (partitionID == 0)
            graphLens.executeSelect(f, graphState) {
              taskManager sendAsync TableBuilt
              logger.debug(
                      s"Job '$jobID' at Partition '$partitionID': Global Select executed on graph with accumulators in ${System
                        .currentTimeMillis() - time}ms."
              )
            }
          else {
            taskManager sendAsync TableBuilt
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Global Select executed on graph with accumulators in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        //TODO create explode select with accumulators
        case ExplodeSelect(f)                                                 =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.explodeSelect(f) {
            taskManager sendAsync TableBuilt
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Exploded Select executed on graph in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case TableFilter(f)                                                   =>
          val time = System.currentTimeMillis()
          graphLens.filteredTable(f) {
            taskManager sendAsync TableFunctionComplete
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Table Filter executed on table in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case Explode(f)                                                       =>
          val time = System.currentTimeMillis()
          graphLens.explodeTable(f) {
            taskManager sendAsync TableFunctionComplete
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Table Explode executed on table in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case WriteTo(outputFormat)                                            =>
          val time     = System.currentTimeMillis()
          val producer =
            if (outputFormat.isInstanceOf[PulsarOutputFormat])
              Some(
                      new PulsarConnector(conf).accessClient
                        .newProducer(Schema.STRING)
                        .topic(
                                outputFormat.asInstanceOf[PulsarOutputFormat].pulsarTopic
                        ) // TODO change here : Topic name with deployment
                        .create()
              )
            else
              None

          graphLens
            .getDataTable()
            .foreach(row =>
              outputFormat match {
                case format: PulsarOutputFormat =>
                  format.writeToPulsar(
                          currentTimestamp,
                          currentWindow,
                          jobID,
                          row,
                          partitionID,
                          producer.get
                  )
                case format                     =>
                  format
                    .write(currentTimestamp, currentWindow, jobID, row, partitionID)

              }
            )
          taskManager sendAsync TableFunctionComplete
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Writing Results executed on table in ${System
                    .currentTimeMillis() - time}ms. Results written to '${outputFormat.getClass.getSimpleName}'."
          )

        //TODO Kill this worker once this is received
        case EndQuery(jobID)                                                  =>
          // It's a Warning, but not a severe one
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Received 'EndQuery' message. "
          )

        case _: CheckMessages                                                 =>
          val time = System.currentTimeMillis()
          taskManager sendAsync
            GraphFunctionComplete(
                    partitionID,
                    receivedMessageCount.get(),
                    sentMessageCount.get(),
                    votedToHalt
            )

          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Messages checked in ${System
                    .currentTimeMillis() - time}ms."
          )
      }
    }
    catch {
      case e: Throwable =>
        errorHandler(e)
    }
  }

  def errorHandler(error: Throwable): Unit = {
    error.printStackTrace()
    taskManager sendAsync AlgorithmFailure(error)
  }
}
