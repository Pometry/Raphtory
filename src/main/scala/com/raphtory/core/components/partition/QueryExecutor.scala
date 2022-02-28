package com.raphtory.core.components.partition

import com.raphtory.core.components.querymanager._
import com.raphtory.core.algorithm._
import com.raphtory.core.components.Component
import com.raphtory.core.components.querymanager.CheckMessages
import com.raphtory.core.components.querymanager.CreatePerspective
import com.raphtory.core.components.querymanager.EndQuery
import com.raphtory.core.components.querymanager.ExecutorEstablished
import com.raphtory.core.components.querymanager.GraphFunctionComplete
import com.raphtory.core.components.querymanager.MetaDataSet
import com.raphtory.core.components.querymanager.PerspectiveEstablished
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.SetMetaData
import com.raphtory.core.components.querymanager.TableBuilt
import com.raphtory.core.components.querymanager.TableFunctionComplete
import com.raphtory.core.components.querymanager.VertexMessage
import com.raphtory.core.components.querymanager.VertexMessageBatch
import com.raphtory.core.config.AsyncConsumer
import com.raphtory.core.config.MonixScheduler
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.GraphPartition
import com.raphtory.core.graph.LensInterface
import com.raphtory.core.storage.pojograph.PojoGraphLens
import com.raphtory.core.storage.pojograph.messaging.VertexMessageHandler
import com.raphtory.output.PulsarOutputFormat
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api._

class QueryExecutor(
    partitionID: Int,
    storage: GraphPartition,
    jobID: String,
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends Component[QueryManagement, QueryManagement](conf: Config, pulsarController, scheduler) {

  var graphLens: LensInterface  = _
  var sentMessageCount: Int     = 0
  var receivedMessageCount: Int = 0
  var votedToHalt: Boolean      = false

  private val taskManager: Producer[Array[Byte]]          = toQueryHandlerProducer(jobID)
  private val neighbours: Map[Int, Producer[Array[Byte]]] = toQueryExecutorProducers(jobID)

  override val consumer = Some(
          startQueryExecutorConsumer(partitionID, jobID)
  )

  override def run(): Unit = {
    logger.debug(s"Job '$jobID' at Partition '$partitionID': Starting query executor consumer.")
    sendMessage(taskManager, ExecutorEstablished(partitionID))
    scheduler.execute(AsyncConsumer(this))
  }

  override def stop(): Unit = {
    consumer match {
      case Some(value) =>
        value.close()
    }
    taskManager.close()
    neighbours.foreach(_._2.close())
  }

  override def handleMessage(msg: QueryManagement): Boolean = {
    var scheduleAgain = true
    msg match {

      case VertexMessageBatch(msgBatch)                =>
        logger.trace(
                s"Job '$jobID' at Partition '$partitionID': Executing 'VertexMessageBatch', '[${msgBatch
                  .mkString(",")}]'."
        )
        msgBatch.foreach(message => graphLens.receiveMessage(message))
        receivedMessageCount += msgBatch.size

      case msg: VertexMessage[_]                       =>
        logger.trace(
                s"Job '$jobID' at Partition '$partitionID': Executing 'VertexMessage', '$msg'."
        )
        graphLens.receiveMessage(msg)
        receivedMessageCount += 1

      case CreatePerspective(timestamp, window)        =>
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Creating perspective at time '$timestamp' with window '$window'."
        )
        val lens = PojoGraphLens(
                jobID,
                timestamp,
                window,
                0,
                storage,
                VertexMessageHandler(conf, neighbours)
        )
        graphLens = lens
        sentMessageCount = 0
        receivedMessageCount = 0
        sendMessage(taskManager, PerspectiveEstablished(lens.getSize()))

      case SetMetaData(vertices)                       =>
        logger.debug(
                s"Job $jobID at Partition '$partitionID': Executing 'SetMetaData' function on graph."
        )

        graphLens.setFullGraphSize(vertices)
        //TODO currently no handlers, to be added back?
        sendMessage(taskManager, MetaDataSet)

      case Step(f)                                     =>
        logger.debug(s"Job $jobID at Partition '$partitionID': Executing 'Step' function on graph.")

        graphLens.nextStep()
        graphLens.runGraphFunction(f)

        val sentMessages = graphLens.getMessageHandler().getCount()
        graphLens.getMessageHandler().flushMessages()
        sendMessage(taskManager, GraphFunctionComplete(sentMessages, receivedMessageCount))
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Step function produced and sent '$sentMessages' messages."
        )
        sentMessageCount = sentMessages

      case Iterate(f, iterations, executeMessagedOnly) =>
        graphLens.nextStep()

        if (executeMessagedOnly) {
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Executing 'Iterate' function on messaged vertices only."
          )

          graphLens.runMessagedGraphFunction(f)
        }
        else {
          logger.debug(
                  s"Job '$jobID' at Partition '$partitionID': Executing 'Iterate' function on all vertices."
          )

          graphLens.runGraphFunction(f)
        }

        val sentMessages = graphLens.getMessageHandler().getCount()
        graphLens.getMessageHandler().flushMessages()
        sendMessage(
                taskManager,
                GraphFunctionComplete(receivedMessageCount, sentMessages, graphLens.checkVotes())
        )
        votedToHalt = graphLens.checkVotes()
        sentMessageCount = sentMessages

        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Iterate function produced and sent '$sentMessages' messages."
        )
      case VertexFilter(f)                             =>
        sendMessage(taskManager, GraphFunctionComplete(0, 0))

      case ClearChain()                                =>
        logger.debug(s"Job $jobID at Partition '$partitionID': Executing 'ClearChain' on graph.")
        graphLens.clearMessages()
        sendMessage(taskManager, GraphFunctionComplete(0, 0))

      case Select(f)                                   =>
        logger.debug(s"Job '$jobID' at Partition '$partitionID': Executing 'Select' query on graph")
        graphLens.nextStep()
        graphLens.executeSelect(f)
        sendMessage(taskManager, TableBuilt)

      case ExplodeSelect(f)                            =>
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Executing 'ExplodeSelect' query on graph"
        )
        graphLens.nextStep()
        graphLens.explodeSelect(f)
        sendMessage(taskManager, TableBuilt)

      case TableFilter(f)                              =>
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Executing 'TableFilter' query on graph."
        )
        graphLens.filteredTable(f)
        sendMessage(taskManager, TableFunctionComplete)

      case Explode(f)                                  =>
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Executing 'Explode' query on graph."
        )
        graphLens.explodeTable(f)
        sendMessage(taskManager, TableFunctionComplete)

      case WriteTo(outputFormat)                       =>
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Writing results to '$outputFormat'."
        )
        val producer =
          if (outputFormat.isInstanceOf[PulsarOutputFormat])
            Some(
                    pulsarController.accessClient
                      .newProducer(Schema.STRING)
                      .topic(outputFormat.asInstanceOf[PulsarOutputFormat].pulsarTopic)
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
                        graphLens.getTimestamp(),
                        graphLens.getWindow(),
                        jobID,
                        row,
                        partitionID,
                        producer.get
                )
              case format                     =>
                format
                  .write(graphLens.getTimestamp(), graphLens.getWindow(), jobID, row, partitionID)

            }
          )
        sendMessage(taskManager, TableFunctionComplete)

      case EndQuery(jobID)                             =>
        // It's a Warning, but not a severe one
        if (logger.underlying.isDebugEnabled)
          logger.warn(
                  s"Job '$jobID' at Partition '$partitionID': Received 'EndQuery' message. " +
                    s"This function is not supported yet."
          )
        scheduleAgain = false

      case _: CheckMessages                            =>
        logger.debug(s"Job '$jobID' at Partition '$partitionID': Received 'CheckMessages'.")
        sendMessage(
                taskManager,
                GraphFunctionComplete(receivedMessageCount, sentMessageCount, votedToHalt)
        )

    }
    scheduleAgain
  }

  override def name(): String = s"Query Executor $partitionID $jobID"
}
