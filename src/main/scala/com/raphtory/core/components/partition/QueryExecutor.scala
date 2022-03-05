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
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.GraphPartition
import com.raphtory.core.graph.LensInterface
import com.raphtory.core.storage.pojograph.PojoGraphLens
import com.raphtory.core.storage.pojograph.messaging.VertexMessageHandler
import com.raphtory.output.PulsarOutputFormat
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
    pulsarController: PulsarController
) extends Component[QueryManagement](conf: Config, pulsarController) {

  var graphLens: LensInterface            = _
  var sentMessageCount: AtomicInteger     = new AtomicInteger(0)
  var receivedMessageCount: AtomicInteger = new AtomicInteger(0)
  var votedToHalt: Boolean                = false

  private val taskManager: Producer[Array[Byte]] = pulsarController.toQueryHandlerProducer(jobID)

  private val neighbours: Map[Int, Producer[Array[Byte]]] =
    pulsarController.toQueryExecutorProducers(jobID)
  var cancelableConsumer: Option[Consumer[Array[Byte]]]   = None

  override def run(): Unit = {
    logger.debug(s"Job '$jobID' at Partition '$partitionID': Starting query executor consumer.")

    taskManager sendAsync serialise(ExecutorEstablished(partitionID))
    cancelableConsumer = Some(
            pulsarController
              .startQueryExecutorConsumer(partitionID, jobID, messageListener())
    )
  }

  override def stop(): Unit = {
    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }
    taskManager.close()
    neighbours.foreach(_._2.close())
  }

  override def handleMessage(msg: QueryManagement): Unit = {
    msg match {

      case VertexMessageBatch(msgBatch)                                     =>
        logger.trace(
                s"Job '$jobID' at Partition '$partitionID': Executing 'VertexMessageBatch', '[${msgBatch
                  .mkString(",")}]'."
        )
        msgBatch.foreach(message => graphLens.receiveMessage(message))
        receivedMessageCount.addAndGet(msgBatch.size)

      case msg: VertexMessage[_]                                            =>
        logger.trace(
                s"Job '$jobID' at Partition '$partitionID': Executing 'VertexMessage', '$msg'."
        )
        graphLens.receiveMessage(msg)
        receivedMessageCount.addAndGet(1)

      case CreatePerspective(timestamp, window)                             =>
        val time = System.currentTimeMillis()
        val lens = PojoGraphLens(
                jobID,
                timestamp,
                window,
                superStep = 0,
                storage,
                conf,
                neighbours,
                sentMessageCount,
                receivedMessageCount
        )
        graphLens = lens
        sentMessageCount.set(0)
        receivedMessageCount.set(0)
        taskManager sendAsync serialise(PerspectiveEstablished(lens.getSize()))
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Created perspective at time '$timestamp' with window '$window'. in ${System
                  .currentTimeMillis() - time}ms"
        )

      case SetMetaData(vertices)                                            =>
        val time = System.currentTimeMillis()
        graphLens.setFullGraphSize(vertices)
        taskManager sendAsync serialise(MetaDataSet)
        logger.debug(
                s"Job $jobID at Partition '$partitionID': Meta Data set in ${System.currentTimeMillis() - time}ms"
        )

      case Step(f)                                                          =>
        val time             = System.currentTimeMillis()
        graphLens.nextStep()
        graphLens.runGraphFunction(f)
        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()
        graphLens.getMessageHandler().flushMessages()
        taskManager sendAsync serialise(
                GraphFunctionComplete(partitionID, receivedMessages, sentMessages)
        )
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Step function finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."
        )

      case StepWithGraph(f, graphState)                                     =>
        val time = System.currentTimeMillis()
        graphLens.nextStep()
        graphLens.runGraphFunction(f, graphState)

        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()
        graphLens.getMessageHandler().flushMessages()
        taskManager sendAsync serialise(
                GraphFunctionCompleteWithState(
                        receivedMessages,
                        sentMessages,
                        graphState = graphState
                )
        )
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Step function on graph with accumulators finished in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages."
        )

      case Iterate(f, iterations, executeMessagedOnly)                      =>
        val time             = System.currentTimeMillis()
        graphLens.nextStep()
        if (executeMessagedOnly)
          graphLens.runMessagedGraphFunction(f)
        else
          graphLens.runGraphFunction(f)
        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()
        graphLens.getMessageHandler().flushMessages()
        taskManager sendAsync serialise(
                GraphFunctionComplete(
                        partitionID,
                        receivedMessages,
                        sentMessages,
                        graphLens.checkVotes()
                )
        )
        votedToHalt = graphLens.checkVotes()
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Iterate function completed in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages with `executeMessageOnly` flag set to $executeMessagedOnly."
        )

      case IterateWithGraph(f, iterations, executeMessagedOnly, graphState) =>
        val time             = System.currentTimeMillis()
        graphLens.nextStep()
        if (executeMessagedOnly)
          graphLens.runMessagedGraphFunction(f, graphState)
        else
          graphLens.runGraphFunction(f, graphState)
        val sentMessages     = sentMessageCount.get()
        val receivedMessages = receivedMessageCount.get()
        graphLens.getMessageHandler().flushMessages()
        taskManager sendAsync serialise(
                GraphFunctionCompleteWithState(
                        receivedMessages,
                        sentMessages,
                        graphLens.checkVotes(),
                        graphState
                )
        )
        votedToHalt = graphLens.checkVotes()

        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Iterate function on graph with accumulators completed  in ${System
                  .currentTimeMillis() - time}ms and sent '$sentMessages' messages with `executeMessageOnly` flag set to $executeMessagedOnly."
        )

      //TODO implement
      case VertexFilter(f)                                                  =>
        taskManager sendAsync serialise(GraphFunctionComplete(partitionID, 0, 0))

      //TODO implement
      case VertexFilterWithGraph(f, graphState)                             =>
        taskManager sendAsync serialise(GraphFunctionComplete(partitionID, 0, 0))

      case ClearChain()                                                     =>
        val time = System.currentTimeMillis()
        graphLens.clearMessages()
        taskManager sendAsync serialise(GraphFunctionComplete(partitionID, 0, 0))
        logger.debug(s"Job $jobID at Partition '$partitionID': Messages cleared on graph in ${System
          .currentTimeMillis() - time}ms.")

      case Select(f)                                                        =>
        val time = System.currentTimeMillis()
        graphLens.nextStep()
        graphLens.executeSelect(f)
        taskManager sendAsync serialise(TableBuilt)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Select executed on graph in ${System
                  .currentTimeMillis() - time}ms."
        )

      case SelectWithGraph(f, graphState)                                   =>
        val time = System.currentTimeMillis()
        graphLens.nextStep()
        graphLens.executeSelect(f, graphState)
        taskManager sendAsync serialise(TableBuilt)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Select executed on graph with accumulators in ${System
                  .currentTimeMillis() - time}ms."
        )

      case GlobalSelect(f, graphState)                                      =>
        val time = System.currentTimeMillis()
        graphLens.nextStep()
        if (partitionID == 0)
          graphLens.executeSelect(f, graphState)
        taskManager sendAsync serialise(TableBuilt)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Global Select executed on graph with accumulators in ${System
                  .currentTimeMillis() - time}ms."
        )

      //TODO create explode select with accumulators
      case ExplodeSelect(f)                                                 =>
        val time = System.currentTimeMillis()
        graphLens.nextStep()
        graphLens.explodeSelect(f)
        taskManager sendAsync serialise(TableBuilt)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Exploded Select executed on graph in ${System
                  .currentTimeMillis() - time}ms."
        )

      case TableFilter(f)                                                   =>
        val time = System.currentTimeMillis()
        graphLens.filteredTable(f)
        taskManager sendAsync serialise(TableFunctionComplete)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Table Filter executed on table in ${System
                  .currentTimeMillis() - time}ms."
        )

      case Explode(f)                                                       =>
        val time = System.currentTimeMillis()
        graphLens.explodeTable(f)
        taskManager sendAsync serialise(TableFunctionComplete)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Table Explode executed on table in ${System
                  .currentTimeMillis() - time}ms."
        )

      case WriteTo(outputFormat)                                            =>
        val time     = System.currentTimeMillis()
        val producer =
          if (outputFormat.isInstanceOf[PulsarOutputFormat])
            Some(
                    pulsarController.accessClient
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
        taskManager sendAsync serialise(TableFunctionComplete)
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Writing Results executed on table in ${System
                  .currentTimeMillis() - time}ms. Results written to '$outputFormat'."
        )

      //TODO Kill this worker once this is received
      case EndQuery(jobID)                                                  =>
        // It's a Warning, but not a severe one
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Received 'EndQuery' message. "
        )

      case _: CheckMessages                                                 =>
        val time = System.currentTimeMillis()
        taskManager sendAsync serialise(
                GraphFunctionComplete(
                        partitionID,
                        receivedMessageCount.get(),
                        sentMessageCount.get(),
                        votedToHalt
                )
        )
        logger.debug(
                s"Job '$jobID' at Partition '$partitionID': Messages checked in ${System
                  .currentTimeMillis() - time}ms."
        )
    }
  }

}
