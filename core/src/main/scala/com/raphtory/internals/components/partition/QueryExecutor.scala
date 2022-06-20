package com.raphtory.internals.components.partition

import com.raphtory.api.analysis.graphview.ClearChain
import com.raphtory.api.analysis.graphview.ExplodeSelect
import com.raphtory.api.analysis.graphview.GlobalSelect
import com.raphtory.api.analysis.graphview.Iterate
import com.raphtory.api.analysis.graphview.IterateWithGraph
import com.raphtory.api.analysis.graphview.MultilayerView
import com.raphtory.api.analysis.graphview.ReduceView
import com.raphtory.api.analysis.graphview.Select
import com.raphtory.api.analysis.graphview.SelectWithGraph
import com.raphtory.api.analysis.graphview.Step
import com.raphtory.api.analysis.graphview.StepWithGraph
import com.raphtory.api.analysis.table.Explode
import com.raphtory.api.analysis.table.TableFilter
import com.raphtory.api.analysis.table.WriteToOutput
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.AlgorithmFailure
import com.raphtory.internals.components.querymanager.CheckMessages
import com.raphtory.internals.components.querymanager.CompleteWrite
import com.raphtory.internals.components.querymanager.CreatePerspective
import com.raphtory.internals.components.querymanager.EndQuery
import com.raphtory.internals.components.querymanager.ExecutorEstablished
import com.raphtory.internals.components.querymanager.GenericVertexMessage
import com.raphtory.internals.components.querymanager.GraphFunctionComplete
import com.raphtory.internals.components.querymanager.GraphFunctionCompleteWithState
import com.raphtory.internals.components.querymanager.MetaDataSet
import com.raphtory.internals.components.querymanager.PerspectiveEstablished
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.SetMetaData
import com.raphtory.internals.components.querymanager.TableBuilt
import com.raphtory.internals.components.querymanager.TableFunctionComplete
import com.raphtory.internals.components.querymanager.VertexMessageBatch
import com.raphtory.internals.components.querymanager.WriteCompleted
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.LensInterface
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.pojograph.PojoGraphLens
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicInteger

private[raphtory] class QueryExecutor(
    partitionID: Int,
    sink: Sink,
    storage: GraphPartition,
    jobID: String,
    conf: Config,
    topics: TopicRepository,
    scheduler: Scheduler
) extends Component[QueryManagement](conf) {

  private val logger: Logger                      = Logger(LoggerFactory.getLogger(this.getClass))
  private var currentPerspectiveID: Int           = _
  private var currentPerspective: Perspective     = _
  private var graphLens: LensInterface            = _
  private val sentMessageCount: AtomicInteger     = new AtomicInteger(0)
  private val receivedMessageCount: AtomicInteger = new AtomicInteger(0)
  private var votedToHalt: Boolean                = false

  private val listeningTopics =
    if (totalPartitions > 1) Seq(topics.jobOperations(jobID), topics.vertexMessages(jobID))
    else Seq(topics.jobOperations(jobID))

  private val listener = topics.registerListener(
          s"$deploymentID-$jobID-query-executor-$partitionID",
          handleMessage,
          listeningTopics,
          partitionID
  )

  private val taskManager = topics.jobStatus(jobID).endPoint

  private val sinkExecutor: SinkExecutor = sink.executor(jobID, partitionID, conf)

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
//    sinkExecutor.close()
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

        case CreatePerspective(id, perspective)                               =>
          val time = System.currentTimeMillis()
          val lens = PojoGraphLens(
                  jobID,
                  perspective.actualStart,
                  perspective.actualEnd,
                  superStep = 0,
                  storage,
                  conf,
                  neighbours,
                  sentMessageCount,
                  receivedMessageCount,
                  errorHandler,
                  scheduler
          )
          currentPerspectiveID = id
          currentPerspective = perspective
          graphLens = lens
          sentMessageCount.set(0)
          receivedMessageCount.set(0)
          taskManager sendAsync PerspectiveEstablished(currentPerspectiveID, lens.getSize)
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Created perspective at time '${perspective.timestamp}' with window '${perspective.window}'. in ${System
                    .currentTimeMillis() - time}ms"
          )

        case SetMetaData(vertices)                                            =>
          val time = System.currentTimeMillis()
          graphLens.setFullGraphSize(vertices)
          taskManager sendAsync MetaDataSet(currentPerspectiveID)
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
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

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
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

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
                GraphFunctionComplete(
                        currentPerspectiveID,
                        partitionID,
                        receivedMessages,
                        sentMessages
                )

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
                        currentPerspectiveID,
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
                        currentPerspectiveID,
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
                        currentPerspectiveID,
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
          taskManager sendAsync GraphFunctionComplete(currentPerspectiveID, partitionID, 0, 0)
          logger.debug(
                  s"Job $jobID at Partition '$partitionID': Messages cleared on graph in ${System
                    .currentTimeMillis() - time}ms."
          )

        case Select(f)                                                        =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.executeSelect(f) {
            taskManager sendAsync TableBuilt(currentPerspectiveID)
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Select executed on graph in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case SelectWithGraph(f, graphState)                                   =>
          val time = System.currentTimeMillis()
          graphLens.nextStep()
          graphLens.executeSelect(f, graphState) {
            taskManager sendAsync TableBuilt(currentPerspectiveID)
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
              taskManager sendAsync TableBuilt(currentPerspectiveID)
              logger.debug(
                      s"Job '$jobID' at Partition '$partitionID': Global Select executed on graph with accumulators in ${System
                        .currentTimeMillis() - time}ms."
              )
            }
          else {
            taskManager sendAsync TableBuilt(currentPerspectiveID)
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
            taskManager sendAsync TableBuilt(currentPerspectiveID)
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Exploded Select executed on graph in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case TableFilter(f)                                                   =>
          val time = System.currentTimeMillis()
          graphLens.filteredTable(f) {
            taskManager sendAsync TableFunctionComplete(currentPerspectiveID)
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Table Filter executed on table in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case Explode(f)                                                       =>
          val time = System.currentTimeMillis()
          graphLens.explodeTable(f) {
            taskManager sendAsync TableFunctionComplete(currentPerspectiveID)
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Table Explode executed on table in ${System
                      .currentTimeMillis() - time}ms."
            )
          }

        case WriteToOutput                                                    =>
          val time = System.currentTimeMillis()

          sinkExecutor.setupPerspective(currentPerspective)
          val writer = row => sinkExecutor.threadSafeWriteRow(row)
          graphLens.writeDataTable(writer) {
            sinkExecutor.closePerspective()
            taskManager sendAsync TableFunctionComplete(currentPerspectiveID)
            logger.debug(
                    s"Job '$jobID' at Partition '$partitionID': Writing Results executed on table in ${System
                      .currentTimeMillis() - time}ms. Results written to '${sink.getClass.getSimpleName}'."
            )
          }

        case CompleteWrite                                                    =>
          sinkExecutor.close()
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Received 'CompleteWrite' message. " +
                    s"Output writer was successfully closed"
          )
          taskManager sendAsync WriteCompleted

        //TODO Kill this worker once this is received
        case EndQuery(jobID)                                                  =>
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Received 'EndQuery' message. "
          )

        case _: CheckMessages                                                 =>
          val time = System.currentTimeMillis()
          taskManager sendAsync
            GraphFunctionComplete(
                    currentPerspectiveID,
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
    taskManager sendAsync AlgorithmFailure(currentPerspectiveID, error)
  }
}
