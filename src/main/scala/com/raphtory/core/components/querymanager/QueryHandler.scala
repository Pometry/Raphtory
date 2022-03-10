package com.raphtory.core.components.querymanager

import com.raphtory.core.graph.PerspectiveController.DEFAULT_PERSPECTIVE_TIME
import com.raphtory.core.graph.PerspectiveController.DEFAULT_PERSPECTIVE_WINDOW
import Stages.SpawnExecutors
import Stages.Stage
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm._
import com.raphtory.core.components.Component
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.Perspective
import com.raphtory.core.graph.PerspectiveController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec

/** @DoNotDocument */
abstract class QueryHandler(
    queryManager: QueryManager,
    scheduler: Scheduler,
    jobID: String,
    algorithm: GraphAlgorithm,
    outputFormat: OutputFormat,
    conf: Config,
    pulsarController: PulsarController
) extends Component[QueryManagement](conf: Config, pulsarController: PulsarController) {

  private val self: Producer[Array[Byte]]    = pulsarController.toQueryHandlerProducer(jobID)
  private val readers: Producer[Array[Byte]] = pulsarController.toReaderProducer
  private val tracker: Producer[Array[Byte]] = pulsarController.toQueryTrackerProducer(jobID)

  private val workerList: Map[Int, Producer[Array[Byte]]] =
    pulsarController.toQueryExecutorProducers(jobID)

  private var perspectiveController: PerspectiveController = _
  private var graphPerspective: GenericGraphPerspective    = _
  private var table: GenericTable                          = _
  private var currentOperation: GraphFunction              = _
  private var graphState: GraphStateImplementation         = _

  private var currentPerspective: Perspective =
    Perspective(DEFAULT_PERSPECTIVE_TIME, DEFAULT_PERSPECTIVE_WINDOW)

  private var lastTime: Long            = 0L
  private var readyCount: Int           = 0
  private var vertexCount: Int          = 0
  private var receivedMessageCount: Int = 0
  private var sentMessageCount: Int     = 0
  private var allVoteToHalt: Boolean    = true
  private var timeTaken                 = System.currentTimeMillis()

  protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController

  private var currentState: Stage = SpawnExecutors

  class RecheckTimer extends Runnable {

    def run(): Unit =
      self sendAsync serialise(RecheckTime)
  }
  private val recheckTimer                              = new RecheckTimer()
  var cancelableConsumer: Option[Consumer[Array[Byte]]] = None

  override def run(): Unit = {
    readers sendAsync serialise(EstablishExecutor(jobID))
    timeTaken = System.currentTimeMillis() //Set time from the point we ask the executors to set up

    cancelableConsumer = Some(
            pulsarController.startQueryHandlerConsumer(jobID, messageListener())
    )

    logger.debug(s"Job '$jobID': Starting query handler consumer.")
  }

  override def stop(): Unit = {
    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }
    self.close()
    readers.close()
    tracker.close()
    workerList.foreach(_._2.close())
  }

  override def handleMessage(msg: QueryManagement): Unit =
    currentState match {
      case Stages.SpawnExecutors       =>
        currentState = spawnExecutors(msg.asInstanceOf[ExecutorEstablished])
      case Stages.EstablishPerspective => currentState = establishPerspective(msg)
      case Stages.ExecuteGraph         => currentState = executeGraph(msg)
      case Stages.ExecuteTable         => currentState = executeTable(msg)
      case Stages.EndTask              => //TODO?
    }

  ////OPERATION STATES
  //Communicate with all readers and get them to spawn a QueryExecutor for their partition
  private def spawnExecutors(msg: ExecutorEstablished): Stage = {
    val workerID = msg.worker
    logger.trace(s"Job '$jobID': Deserialized worker '$workerID'.")

    if (readyCount + 1 == totalPartitions) {
      val latestTime          = whatsTheTime()
      perspectiveController = buildPerspectiveController(latestTime)
      val schedulingTimeTaken = System.currentTimeMillis() - timeTaken
      logger.debug(s"Job '$jobID': Spawned all executors in ${schedulingTimeTaken}ms.")
      timeTaken = System.currentTimeMillis()
      readyCount = 0
      executeNextPerspective()
    }
    else {
      readyCount += 1
      Stages.SpawnExecutors
    }
  }

  //build the perspective within the QueryExecutor for each partition -- the view to be analysed
  private def establishPerspective(msg: QueryManagement): Stage =
    msg match {
      case RecheckTime               =>
        logger.trace(s"Job '$jobID': Rechecking time of $currentPerspective.")
        recheckTime(currentPerspective)

      case p: PerspectiveEstablished =>
        vertexCount += p.vertices
        readyCount += 1
        if (readyCount == totalPartitions) {
          readyCount = 0
          messagetoAllJobWorkers(SetMetaData(vertexCount))
          val establishingPerspectiveTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Perspective Established in ${establishingPerspectiveTimeTaken}ms. Messaging all workers: vertex count: $vertexCount."
          )
          timeTaken = System.currentTimeMillis()
        }
        Stages.EstablishPerspective

      case MetaDataSet               =>
        if (readyCount + 1 == totalPartitions) {
          self sendAsync serialise(StartGraph)
          readyCount = 0
          receivedMessageCount = 0
          sentMessageCount = 0
          currentOperation = null
          allVoteToHalt = true
          val settingMetaDataTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Setting MetaData took ${settingMetaDataTimeTaken}ms. Executing graph with windows '${currentPerspective.window}' " +
                    s"at timestamp '${currentPerspective.timestamp}'."
          )
          timeTaken = System.currentTimeMillis()
          Stages.ExecuteGraph
        }
        else {
          readyCount += 1
          Stages.EstablishPerspective
        }
    }

  //execute the steps of the graph algorithm until a select is run
  private def executeGraph(msg: QueryManagement): Stage =
    msg match {
      case StartGraph                                                                         =>
        graphPerspective = new GenericGraphPerspective(vertexCount)
        graphState = GraphStateImplementation()
        val startingGraphTime = System.currentTimeMillis() - timeTaken
        logger.debug(
                s"Job '$jobID': Sending self GraphStart took ${startingGraphTime}ms. Running '${algorithm.getClass.getSimpleName}'."
        )
        algorithm.run(graphPerspective)
        timeTaken = System.currentTimeMillis()
        table = graphPerspective.getTable()
        table.writeTo(outputFormat) //sets output formatter

        nextGraphOperation(vertexCount)

      case GraphFunctionCompleteWithState(receivedMessages, sentMessages, votedToHalt, state) =>
        sentMessageCount += sentMessages
        receivedMessageCount += receivedMessages
        allVoteToHalt = votedToHalt & allVoteToHalt
        readyCount += 1
        graphState.update(state)

        if (readyCount == totalPartitions) {
          graphState.rotate()
          if (receivedMessageCount == sentMessageCount) {
            val graphFuncCompleteTime = System.currentTimeMillis() - timeTaken
            logger.debug(
                    s"Job '$jobID': Graph Function Complete in ${graphFuncCompleteTime}ms Received messages:$receivedMessageCount , Sent messages: $sentMessageCount."
            )
            timeTaken = System.currentTimeMillis()
            nextGraphOperation(vertexCount)
          }
          else {
            readyCount = 0
            receivedMessageCount = 0
            sentMessageCount = 0
            messagetoAllJobWorkers(CheckMessages(jobID))
            Stages.ExecuteGraph
          }
        }
        else
          Stages.ExecuteGraph

      case GraphFunctionComplete(partitionID, receivedMessages, sentMessages, votedToHalt)    =>
        sentMessageCount += sentMessages
        receivedMessageCount += receivedMessages
        allVoteToHalt = votedToHalt & allVoteToHalt
        readyCount += 1
        logger.debug(
                s"Job '$jobID': Partition $partitionID Received messages:$receivedMessages , Sent messages: $sentMessages."
        )
        if (readyCount == totalPartitions)
          if (receivedMessageCount == sentMessageCount) {
            val graphFuncCompleteTime = System.currentTimeMillis() - timeTaken
            logger.debug(
                    s"Job '$jobID': Graph Function Complete in ${graphFuncCompleteTime}ms Received messages Total:$receivedMessageCount , Sent messages: $sentMessageCount."
            )
            timeTaken = System.currentTimeMillis()
            nextGraphOperation(vertexCount)
          }
          else {
            logger.debug(
                    s"Job '$jobID': Checking messages - Received messages total:$receivedMessageCount , Sent messages total: $sentMessageCount."
            )
            readyCount = 0
            receivedMessageCount = 0
            sentMessageCount = 0
            messagetoAllJobWorkers(CheckMessages(jobID))
            Stages.ExecuteGraph
          }
        else
          Stages.ExecuteGraph
    }

  //once the select has been run, execute all of the table functions until we hit a writeTo
  private def executeTable(msg: QueryManagement): Stage =
    msg match {
      case TableBuilt            =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          readyCount = 0
          val tableBuiltTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Table Built in ${tableBuiltTimeTaken}ms Executing next table operation."
          )
          timeTaken = System.currentTimeMillis()
          nextTableOperation()
        }
        else
          Stages.ExecuteTable

      case TableFunctionComplete =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          val tableFuncTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Table Function complete in ${tableFuncTimeTaken}ms. Running next table operation."
          )
          timeTaken = System.currentTimeMillis()
          nextTableOperation()
        }
        else {
          logger.trace(
                  s"Job '$jobID': Executing '${currentOperation.getClass.getSimpleName}' operation."
          )
          Stages.ExecuteTable
        }
    }
  ////END OPERATION STATES

  ///HELPER FUNCTIONS
  private def executeNextPerspective(): Stage = {
    val latestTime = whatsTheTime()
    if (currentPerspective.timestamp != -1) //ignore initial placeholder
      tracker.sendAsync(serialise(currentPerspective))
    perspectiveController.nextPerspective() match {
      case Some(perspective) if perspective.timestamp <= latestTime =>
        logTotalTimeTaken(perspective)
        messagetoAllJobWorkers(CreatePerspective(perspective.timestamp, perspective.window))
        currentPerspective = perspective
        graphState = GraphStateImplementation()
        graphPerspective = null
        table = null
        val localPerspectiveSetupTime = System.currentTimeMillis() - timeTaken
        logger.debug(
                s"Job '$jobID': Perspective '$perspective' is starting. Took ${localPerspectiveSetupTime}ms"
        )
        timeTaken = System.currentTimeMillis()
        Stages.EstablishPerspective
      case Some(perspective)                                        =>
        logger.trace(
                s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$latestTime'."
        )
        logTotalTimeTaken(perspective)
        currentPerspective = perspective
        graphPerspective = null
        table = null
        scheduler.scheduleOnce(1, TimeUnit.SECONDS, recheckTimer)

        Stages.EstablishPerspective
      case None                                                     =>
        logger.debug(s"Job '$jobID': No more perspectives to run.")
        killJob()
        Stages.EndTask
    }
  }

  private def logTotalTimeTaken(perspective: Perspective): Unit = {
    if (currentPerspective.timestamp != DEFAULT_PERSPECTIVE_TIME)
      currentPerspective.window match {
        case Some(window) =>
          logger.debug(
                  s"Job '$jobID': Perspective at Time '${currentPerspective.timestamp}' with " +
                    s"Window $window took ${System.currentTimeMillis() - lastTime} ms to run."
          )
        case None         =>
          logger.debug(
                  s"Job '$jobID': Perspective at Time '${currentPerspective.timestamp}' " +
                    s"took ${System.currentTimeMillis() - lastTime} ms to run. "
          )
      }
    lastTime = System.currentTimeMillis()
  }

  private def recheckTime(perspective: Perspective): Stage = {
    val time = whatsTheTime()
    timeTaken = System.currentTimeMillis()
    if (perspective.timestamp <= time) {
      logger.debug(s"Job '$jobID': Created perspective at time $time.")

      messagetoAllJobWorkers(CreatePerspective(perspective.timestamp, perspective.window))
      Stages.EstablishPerspective
    }
    else {
      logger.trace(s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$time'.")
      scheduler.scheduleOnce(1, TimeUnit.SECONDS, recheckTimer)
      Stages.EstablishPerspective
    }
  }

  @tailrec
  private def nextGraphOperation(vertexCount: Int): Stage = {
    readyCount = 0
    receivedMessageCount = 0
    sentMessageCount = 0

    currentOperation match {
      case Iterate(f, iterations, executeMessagedOnly) if iterations > 1 && !allVoteToHalt =>
        currentOperation = Iterate(f, iterations - 1, executeMessagedOnly)
      case IterateWithGraph(f, iterations, executeMessagedOnly, _)
          if iterations > 1 && !allVoteToHalt =>
        currentOperation = IterateWithGraph(f, iterations - 1, executeMessagedOnly)
      case _                                                                               =>
        currentOperation = graphPerspective.getNextOperation()
    }
    allVoteToHalt = true

    logger.debug(
            s"Job '$jobID': Executing graph function '${currentOperation.getClass.getSimpleName}'."
    )
    currentOperation match {
      case f: Iterate                                                =>
        messagetoAllJobWorkers(f)
        Stages.ExecuteGraph

      case IterateWithGraph(fun, iterations, executeMessagedOnly, _) =>
        messagetoAllJobWorkers(
                IterateWithGraph(
                        fun,
                        iterations,
                        executeMessagedOnly,
                        graphState
                )
        )
        Stages.ExecuteGraph

      case f: Step                                                   =>
        messagetoAllJobWorkers(f)
        Stages.ExecuteGraph

      case StepWithGraph(fun, _)                                     =>
        messagetoAllJobWorkers(StepWithGraph(fun, graphState))
        Stages.ExecuteGraph

      case f: ClearChain                                             =>
        messagetoAllJobWorkers(f)
        Stages.ExecuteGraph

      case PerspectiveDone()                                         =>
        logger.debug(
                s"Job '$jobID': Executing next perspective with windows '${currentPerspective.window}'" +
                  s" and timestamp '${currentPerspective.timestamp}'."
        )
        executeNextPerspective()

      case f: Select                                                 =>
        messagetoAllJobWorkers(f)
        Stages.ExecuteTable

      case SelectWithGraph(fun, _)                                   =>
        messagetoAllJobWorkers(SelectWithGraph(fun, graphState))
        Stages.ExecuteTable

      case GlobalSelect(f, _)                                        =>
        messagetoAllJobWorkers(GlobalSelect(f, graphState))
        Stages.ExecuteTable

      case f: ExplodeSelect                                          =>
        messagetoAllJobWorkers(f)
        Stages.ExecuteTable

      case Setup(fun)                                                =>
        fun(graphState)
        nextGraphOperation(vertexCount)

      case VertexFilterWithGraph(fun, _)                             =>
        messagetoAllJobWorkers(VertexFilterWithGraph(fun, graphState))
        Stages.ExecuteGraph
    }
  }

  private def nextTableOperation(): Stage =
    table.getNextOperation() match {

      case Some(f: TableFunction) =>
        messagetoAllJobWorkers(f)
        readyCount = 0
        logger.debug(s"Job '$jobID': Executing table function '${f.getClass.getSimpleName}'.")

        Stages.ExecuteTable

      case None                   =>
        readyCount = 0
        receivedMessageCount = 0
        sentMessageCount = 0
        logger.debug(s"Job '$jobID': Executing next perspective.")

        executeNextPerspective()
    }

  private def messagetoAllJobWorkers(msg: QueryManagement): Unit =
    workerList.values.foreach(worker => worker sendAsync serialise(msg))

  private def killJob() = {
    messagetoAllJobWorkers(EndQuery(jobID))
    logger.debug(s"Job '$jobID': No more perspectives available. Ending Query Handler execution.")

    tracker
      .flushAsync()
      .thenApply(_ => tracker.sendAsync(serialise(JobDone)).thenApply(_ => tracker.closeAsync()))

  }

  def whatsTheTime(): Long = queryManager.whatsTheTime()

}

object Stages extends Enumeration {
  type Stage = Value
  val SpawnExecutors, EstablishPerspective, ExecuteGraph, ExecuteTable, EndTask = Value
}
