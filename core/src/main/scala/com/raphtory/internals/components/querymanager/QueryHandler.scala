package com.raphtory.internals.components.querymanager

import Stages.SpawnExecutors
import Stages.Stage
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview.ClearChain
import com.raphtory.api.analysis.graphview.ExplodeSelect
import com.raphtory.api.analysis.graphview.GlobalSelect
import com.raphtory.api.analysis.graphview.GraphFunction
import com.raphtory.api.analysis.graphview.Iterate
import com.raphtory.api.analysis.graphview.IterateWithGraph
import com.raphtory.api.analysis.graphview.MultilayerView
import com.raphtory.api.analysis.graphview.PerspectiveDone
import com.raphtory.api.analysis.graphview.ReduceView
import com.raphtory.api.analysis.graphview.Select
import com.raphtory.api.analysis.graphview.SelectWithGraph
import com.raphtory.api.analysis.graphview.SetGlobalState
import com.raphtory.api.analysis.graphview.Step
import com.raphtory.api.analysis.graphview.StepWithGraph
import com.raphtory.api.analysis.table.TableFunction
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.raphtory.internals.components.Component
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.graph.PerspectiveController
import com.raphtory.internals.graph.PerspectiveController.DEFAULT_PERSPECTIVE_TIME
import com.raphtory.internals.graph.PerspectiveController.DEFAULT_PERSPECTIVE_WINDOW
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.serialisers.KryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.util.Try

private[raphtory] class QueryHandler(
    queryManager: QueryManager,
    scheduler: Scheduler,
    jobID: String,
    query: Query,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val self           = topics.rechecks(jobID).endPoint
  private val readers        = topics.queryPrep.endPoint
  private val tracker        = topics.queryTrack(jobID).endPoint
  private val workerList     = topics.jobOperations(jobID).endPoint

  override def stop(): Unit = {
    listener.close()
    self.close()
    readers.close()
    tracker.close()
    workerList.close()
  }

  private val listener =
    topics.registerListener(
            s"$deploymentID-$jobID-query-handler",
            handleMessage,
            Seq(topics.rechecks(jobID), topics.jobStatus(jobID))
    )

  private var perspectiveController: PerspectiveController = _
  private var graphFunctions: mutable.Queue[GraphFunction] = _
  private var tableFunctions: mutable.Queue[TableFunction] = _
  private var currentOperation: GraphFunction              = _
  private var graphState: GraphStateImplementation         = _
  private var currentPerspectiveID: Int                    = 0

  private var currentPerspective: Perspective =
    Perspective(DEFAULT_PERSPECTIVE_TIME, DEFAULT_PERSPECTIVE_WINDOW, 0, 0)

  private var lastTime: Long            = 0L
  private var readyCount: Int           = 0
  private var vertexCount: Int          = 0
  private var receivedMessageCount: Int = 0
  private var sentMessageCount: Int     = 0
  private var checkingMessages: Boolean = false
  private var allVoteToHalt: Boolean    = true
  private var timeTaken                 = System.currentTimeMillis()

  private var currentState: Stage = SpawnExecutors

  private def recheckTimer(): Unit         = self sendAsync RecheckTime
  private def recheckEarliestTimer(): Unit = self sendAsync RecheckEarliestTime

  override def run(): Unit = {
    messageReader(EstablishExecutor(jobID, query.sink.get))
    timeTaken = System.currentTimeMillis() //Set time from the point we ask the executors to set up
    logger.debug(s"Job '$jobID': Starting query handler consumer.")
    listener.start()
  }

  override def handleMessage(msg: QueryManagement): Unit =
    try msg match {
      case msg: PerspectiveStatus if msg.perspectiveID != currentPerspectiveID =>
      case AlgorithmFailure(_, exception)                                      => throw exception
      case _                                                                   =>
        currentState match {
          case Stages.SpawnExecutors       => currentState = spawnExecutors(msg)
          case Stages.EstablishPerspective => currentState = establishPerspective(msg)
          case Stages.ExecuteGraph         => currentState = executeGraph(msg)
          case Stages.ExecuteTable         => currentState = executeTable(msg)
          case Stages.EndTask              => currentState = endTask(msg)
        }
    }
    catch {
      case e: Throwable =>
        logger.error(
                s"Deployment $deploymentID: Failed to handle message. ${e.getMessage}. Skipping perspective.",
                e
        )
        currentState = executeNextPerspective()
    }

  ////OPERATION STATES
  //Communicate with all readers and get them to spawn a QueryExecutor for their partition
  private def spawnExecutors(msg: QueryManagement): Stage =
    msg match {
      case msg: ExecutorEstablished =>
        val workerID = msg.worker
        logger.trace(s"Job '$jobID': Deserialized worker '$workerID'.")

        if (readyCount + 1 == totalPartitions)
          safelyStartPerspectives()
        else {
          readyCount += 1
          Stages.SpawnExecutors
        }
      case RecheckEarliestTime      => safelyStartPerspectives()
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
          telemetry.graphSizeCollector.labels(jobID).inc(vertexCount)
          messagetoAllJobWorkers(SetMetaData(vertexCount))
          val establishingPerspectiveTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Perspective Established in ${establishingPerspectiveTimeTaken}ms. Messaging all workers: vertex count: $vertexCount."
          )
          timeTaken = System.currentTimeMillis()
        }
        Stages.EstablishPerspective

      case MetaDataSet(_)            =>
        if (readyCount + 1 == totalPartitions) {
          self sendAsync StartGraph
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
        graphFunctions = mutable.Queue.from(query.graphFunctions)
        tableFunctions = mutable.Queue.from(query.tableFunctions)
        graphState = GraphStateImplementation(vertexCount)
        val startingGraphTime = System.currentTimeMillis() - timeTaken
        logger.debug(
                s"Job '$jobID': Sending self GraphStart took ${startingGraphTime}ms. Executing next graph operation."
        )
        timeTaken = System.currentTimeMillis()

        nextGraphOperation(vertexCount)

      case GraphFunctionCompleteWithState(
                  _,
                  partitionID,
                  receivedMessages,
                  sentMessages,
                  votedToHalt,
                  state
          ) =>
        graphState.update(state)
        if (readyCount + 1 == totalPartitions)
          graphState.rotate()
        processGraphFunctionComplete(partitionID, receivedMessages, sentMessages, votedToHalt)

      case GraphFunctionComplete(_, partitionID, receivedMessages, sentMessages, votedToHalt) =>
        processGraphFunctionComplete(partitionID, receivedMessages, sentMessages, votedToHalt)

    }

  private def processGraphFunctionComplete(
      partitionID: Int,
      receivedMessages: Int,
      sentMessages: Int,
      votedToHalt: Boolean
  ) = {
    sentMessageCount += sentMessages
    telemetry.totalSentMessageCount.labels(jobID, deploymentID).inc(sentMessages)
    receivedMessageCount += receivedMessages
    telemetry.receivedMessageCountCollector.labels(jobID, deploymentID).inc(receivedMessages)
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
        if (checkingMessages && topics.jobOperationsConnector.isInstanceOf[PulsarConnector]) { // TODO: clean up later this section
          val pulsarConnector = topics.jobOperationsConnector.asInstanceOf[PulsarConnector]
          val pulsarEndPoint  =
            workerList.asInstanceOf[pulsarConnector.PulsarEndPoint[QueryManagement]]
          val topic           = pulsarEndPoint.producer.getTopic
          logger.debug(s"Checking messages for topic $topic")
          val consumer        = pulsarConnector.createExclusiveConsumer("dumping", Schema.BYTES, topic)
          var has_message     = true
          while (has_message) {
            val msg =
              consumer.receive(
                      10,
                      TimeUnit.SECONDS
              ) // add some timeout to see if new messages come in
            if (msg == null)
              has_message = false
            else {
              val message = KryoSerialiser().deserialise[QueryManagement](msg.getValue)
              consumer.acknowledge(msg)
              msg.release()
              logger.debug(s"Read message $message")
            }
          }
        }
        readyCount = 0
        receivedMessageCount = 0
        sentMessageCount = 0
        checkingMessages = true
        messagetoAllJobWorkers(CheckMessages(jobID))
        Stages.ExecuteGraph
      }
    else
      Stages.ExecuteGraph
  }

  //once the select has been run, execute all of the table functions until we hit a writeTo
  private def executeTable(msg: QueryManagement): Stage =
    msg match {
      case TableBuilt(_)            =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          readyCount = 0
          val tableBuiltTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Table Built in ${tableBuiltTimeTaken}ms Executing next table operation."
          )
          timeTaken = System.currentTimeMillis()
          telemetry.totalTableOperations.labels(jobID, deploymentID).inc()
          nextTableOperation()
        }
        else
          Stages.ExecuteTable

      case TableFunctionComplete(_) =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          val tableFuncTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Table Function complete in ${tableFuncTimeTaken}ms. Running next table operation."
          )
          timeTaken = System.currentTimeMillis()
          telemetry.totalTableOperations.labels(jobID, deploymentID).inc()
          nextTableOperation()
        }
        else {
          logger.trace(
                  s"Job '$jobID': Executing '${currentOperation.getClass.getSimpleName}' operation."
          )
          Stages.ExecuteTable
        }
    }

  private def endTask(msg: QueryManagement): Stage =
    msg match {
      case WriteCompleted =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          readyCount = 0
          val writeCompletedTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(
                  s"Job '$jobID': Write completed in ${writeCompletedTimeTaken}ms."
          )
          timeTaken = System.currentTimeMillis()
          killJob()
          Stages.EndTask
        }
        else
          Stages.EndTask
    }
  ////END OPERATION STATES

  ///HELPER FUNCTIONS
  private def safelyStartPerspectives(): Stage =
    getOptionalEarliestTime match {
      case None               =>
        scheduler.scheduleOnce(1.seconds, recheckEarliestTimer())
        Stages.SpawnExecutors
      case Some(earliestTime) =>
        if (earliestTime > getLatestTime) {
          scheduler.scheduleOnce(1.seconds, recheckEarliestTimer())
          Stages.SpawnExecutors
        }
        else {
          perspectiveController = PerspectiveController(earliestTime, getLatestTime, query)
          val schedulingTimeTaken = System.currentTimeMillis() - timeTaken
          logger.debug(s"Job '$jobID': Spawned all executors in ${schedulingTimeTaken}ms.")
          timeTaken = System.currentTimeMillis()
          readyCount = 0
          executeNextPerspective()
        }
    }

  private def executeNextPerspective(): Stage = {
    val latestTime = getLatestTime
    val oldestTime = getOptionalEarliestTime
    telemetry.totalPerspectivesProcessed.labels(jobID, deploymentID).inc()
    if (currentPerspective.timestamp != -1) //ignore initial placeholder
      tracker sendAsync currentPerspective
    currentPerspectiveID += 1
    perspectiveController.nextPerspective() match {
      case Some(perspective) =>
        logger.trace(
                s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$latestTime'."
        )
        logTotalTimeTaken(perspective)
        currentPerspective = perspective
        graphFunctions = null
        tableFunctions = null
        vertexCount = 0
        graphState = GraphStateImplementation.empty
        recheckTime(currentPerspective)
      case None              =>
        logger.debug(s"Job '$jobID': No more perspectives to run.")
        messagetoAllJobWorkers(CompleteWrite)
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
    val time                 = getLatestTime
    val optionalEarliestTime = getOptionalEarliestTime

    timeTaken = System.currentTimeMillis()
    if (perspectiveIsReady(perspective)) {
      logger.debug(s"Job '$jobID': Created perspective at time $time.")

      messagetoAllJobWorkers(CreatePerspective(currentPerspectiveID, perspective))
      Stages.EstablishPerspective
    }
    else {
      logger.debug(s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$time'.")
      scheduler.scheduleOnce(1.seconds, recheckTimer())
      Stages.EstablishPerspective
    }
  }

  def perspectiveIsReady(perspective: Perspective): Boolean = {
    val time = getLatestTime
    perspective.window match {
      case Some(_) => perspective.actualEnd <= time
      case None    => perspective.timestamp <= time
    }
  }

  @tailrec
  private def nextGraphOperation(vertexCount: Int): Stage = {
    readyCount = 0
    receivedMessageCount = 0
    sentMessageCount = 0
    checkingMessages = false
    telemetry.totalGraphOperations.labels(jobID, deploymentID).inc()

    currentOperation match {
      case Iterate(f, iterations, executeMessagedOnly) if iterations > 1 && !allVoteToHalt             =>
        currentOperation = Iterate(f, iterations - 1, executeMessagedOnly)
      case IterateWithGraph(f, iterations, executeMessagedOnly, _) if iterations > 1 && !allVoteToHalt =>
        currentOperation = IterateWithGraph(f, iterations - 1, executeMessagedOnly)
      case _                                                                                           =>
        currentOperation = getNextGraphOperation(graphFunctions).get
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

      case f: MultilayerView                                         =>
        messagetoAllJobWorkers(f)
        Stages.ExecuteGraph

      case f: ReduceView                                             =>
        messagetoAllJobWorkers(f)
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

      case SetGlobalState(fun)                                       =>
        fun(graphState)
        nextGraphOperation(vertexCount)

    }
  }

  private def nextTableOperation(): Stage =
    getNextTableOperation(tableFunctions) match {

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
    workerList sendAsync msg

  private def messageReader(msg: QueryManagement): Unit =
    readers sendAsync msg

  private def killJob() = {
    messagetoAllJobWorkers(EndQuery(jobID))
    workerList.close()
    messageReader(EndQuery(jobID))
    readers.close()

    val queryManager = topics.completedQueries.endPoint
    queryManager closeWithMessage EndQuery(jobID)
    logger.debug(s"Job '$jobID': No more perspectives available. Ending Query Handler execution.")

    tracker closeWithMessage JobDone
  }

  private def getNextGraphOperation(queue: mutable.Queue[GraphFunction]) =
    Try(queue.dequeue()).toOption

  private def getNextTableOperation(queue: mutable.Queue[TableFunction]) =
    Try(queue.dequeue()).toOption

  private def getLatestTime: Long = queryManager.latestTime()

  private def getOptionalEarliestTime: Option[Long] = queryManager.earliestTime()
}

private[raphtory] object Stages extends Enumeration {
  type Stage = Value
  val SpawnExecutors, EstablishPerspective, ExecuteGraph, ExecuteTable, EndTask = Value
}
