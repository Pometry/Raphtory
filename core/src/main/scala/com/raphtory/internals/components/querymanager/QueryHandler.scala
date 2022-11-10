package com.raphtory.internals.components.querymanager

import cats.effect.std.Dispatcher
import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.api.analysis.graphstate.GraphState
import com.raphtory.api.analysis.graphstate.GraphStateImplementation
import com.raphtory.api.analysis.graphview._
import com.raphtory.api.analysis.table.TableFunction
import com.raphtory.api.analysis.visitor.Vertex
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.partition.QueryExecutor.makeExecutor
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.graph.PerspectiveController
import com.raphtory.internals.graph.PerspectiveController._
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.python.EmbeddedPython
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.internals.management.telemetry.TelemetryReporter

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Try

private[raphtory] class QueryHandler[F[_]: Async] private(
    graphID: String,
    querySupervisor: QuerySupervisor[F],
    scheduler: Scheduler,
    jobID: String,
    query: Query,
    conf: Config,
    topics: TopicRepository,
    pyScript: Option[String],
    earliestTime: Long,
    latestTime: Long,
    dispatcher: Dispatcher[F],
    finish: cats.effect.std.Semaphore[F]
) extends Component[QueryManagement](conf) {
  println(s"Creating QueryHandler")
  import com.raphtory.internals.components.querymanager.Stages._

  private val startTime      = System.currentTimeMillis()
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val tracker        = topics.queryTrack(graphID, jobID).endPoint
  private val workerList     = topics.jobOperations(graphID, jobID).endPoint

  pyScript.map(s => EmbeddedPython.global.run(s))

  private val listener =
    topics.registerListener(
            s"$graphID-$jobID-query-handler",
            handleMessage,
            Seq(topics.jobStatus(graphID, jobID))
    )

  private var perspectiveController: PerspectiveController = _
  private var graphFunctions: mutable.Queue[GraphFunction] = _
  private var tableFunctions: mutable.Queue[TableFunction] = _
  private var currentOperation: GraphFunction              = _
  private var graphState: GraphStateImplementation         = _
  private var currentPerspectiveID: Int                    = 0

  private var currentPerspective: Perspective =
    Perspective(DEFAULT_PERSPECTIVE_TIME, DEFAULT_PERSPECTIVE_WINDOW, 0, 0)

  private var lastTime: Long             = 0L
  private var readyCount: Int            = 4
  private var vertexCount: Int           = 0
  private var receivedMessageCount: Long = 0
  private var sentMessageCount: Long     = 0
  private var checkingMessages: Boolean  = false
  private var allVoteToHalt: Boolean     = true
  private var timeTaken                  = System.currentTimeMillis()

  private var currentState: Stage = SpawnExecutors

  logger.debug(s"Spawned QueryHandler for $jobID in ${System.currentTimeMillis() - startTime}ms")

  override def stop(): Unit = {
    listener.close()
    tracker.close()
    workerList.close()
  }

  override def run(): Unit = {
    timeTaken = System.currentTimeMillis() //Set time from the point we ask the executors to set up
    logger.debug(s"Job '$jobID': Starting query handler consumer.")
    listener.start()
    currentState = safelyStartPerspectives()
  }

  override def handleMessage(msg: QueryManagement): Unit =
    try msg match {
      case msg: PerspectiveStatus if msg.perspectiveID != currentPerspectiveID =>
      case AlgorithmFailure(_, exception)                                      => throw exception
      case _                                                                   =>
        currentState match {
          case Stages.EstablishPerspective => currentState = establishPerspective(msg)
          case Stages.ExecuteGraph         => currentState = executeGraph(msg)
          case Stages.ExecuteTable         => currentState = executeTable(msg)
          case Stages.EndTask              => currentState = endTask(msg)
        }
    }
    catch {
      case e: Throwable =>
        logger.error(
                s"Deployment '$graphID': Failed to handle message. ${e.getMessage}. Skipping perspective.",
                e
        )
        currentState = executeNextPerspective(previousFailing = true, e.getMessage)
    }

  // OPERATION STATES
  // Build the perspective within the QueryExecutor for each partition -- the view to be analysed
  private def establishPerspective(msg: QueryManagement): Stage =
    msg match {
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

      case MetaDataSet(_)            =>
        if (readyCount + 1 == totalPartitions) {
          executeGraph(StartGraph)
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
      receivedMessages: Long,
      sentMessages: Long,
      votedToHalt: Boolean
  ) = {
    sentMessageCount += sentMessages
    TelemetryReporter.totalSentMessageCount.labels(jobID, graphID).inc(sentMessages.toDouble)
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
        logger.error(
                s"Message check failed: Total received messages: $receivedMessageCount, total sent messages: $sentMessageCount"
        )
        throw new RuntimeException("Message check failed")
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
          TelemetryReporter.totalTableOperations.labels(jobID, graphID).inc()
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
          TelemetryReporter.totalTableOperations.labels(jobID, graphID).inc()
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
  // END OPERATION STATES

  // HELPER FUNCTIONS
  private def safelyStartPerspectives(): Stage =
    if (earliestTime > latestTime)
      throw new Exception(
              s"Latest time (= $latestTime) seen can't be smaller than earliest time seen (= $earliestTime)"
      )
    else {
      perspectiveController = PerspectiveController(earliestTime, latestTime, query)
      val schedulingTimeTaken = System.currentTimeMillis() - timeTaken
      logger.debug(s"Job '$jobID': Spawned all executors in ${schedulingTimeTaken}ms.")
      timeTaken = System.currentTimeMillis()
      readyCount = 0
      executeNextPerspective()
    }

  private def executeNextPerspective(previousFailing: Boolean = false, failureReason: String = ""): Stage = {
    TelemetryReporter.totalPerspectivesProcessed.labels(jobID, graphID).inc()
    if (currentPerspective.timestamp != -1) { //ignore initial placeholder
      val report =
        if (previousFailing) PerspectiveFailed(currentPerspective, failureReason)
        else PerspectiveCompleted(currentPerspective)
      tracker sendAsync report
      logTotalTimeTaken()
    }
    currentPerspectiveID += 1
    perspectiveController.nextPerspective() match {
      case Some(perspective) =>
        logger.trace(
                s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$latestTime'."
        )
        currentPerspective = perspective
        graphFunctions = null
        tableFunctions = null
        vertexCount = 0
        graphState = GraphStateImplementation.empty
        timeTaken = System.currentTimeMillis()
        logger.debug(s"Job '$jobID': Created perspective at time ${perspective.timestamp}.")
        lastTime = System.currentTimeMillis()
        messagetoAllJobWorkers(CreatePerspective(currentPerspectiveID, perspective))
        Stages.EstablishPerspective
      case None              =>
        logger.debug(s"Job '$jobID': No more perspectives to run.")
        messagetoAllJobWorkers(CompleteWrite)
        Stages.EndTask
    }
  }

  private def logTotalTimeTaken(): Unit =
    if (currentPerspective.timestamp != DEFAULT_PERSPECTIVE_TIME)
      currentPerspective.window match {
        case Some(window) =>
          logger.info(
                  s"Job '$jobID': Perspective at Time '${currentPerspective.timestamp}' with " +
                    s"Window $window took ${System.currentTimeMillis() - lastTime} ms to run."
          )
        case None         =>
          logger.info(
                  s"Job '$jobID': Perspective at Time '${currentPerspective.timestamp}' " +
                    s"took ${System.currentTimeMillis() - lastTime} ms to run. "
          )
      }

  @tailrec
  private def nextGraphOperation(vertexCount: Int): Stage = {
    readyCount = 0
    receivedMessageCount = 0
    sentMessageCount = 0
    checkingMessages = false
    TelemetryReporter.totalGraphOperations.labels(jobID, graphID).inc()

    currentOperation match {
      case Iterate(f: (Vertex => Unit) @unchecked, iterations, executeMessagedOnly)
          if iterations > 1 && !allVoteToHalt =>
        currentOperation = Iterate(f, iterations - 1, executeMessagedOnly)
      case IterateWithGraph(f: ((Vertex, GraphState) => Unit) @unchecked, iterations, executeMessagedOnly)
          if iterations > 1 && !allVoteToHalt =>
        currentOperation = IterateWithGraph(f, iterations - 1, executeMessagedOnly)
      case _ =>
        currentOperation = getNextGraphOperation(graphFunctions).get
    }
    allVoteToHalt = true

    logger.debug(
            s"Job '$jobID': Executing graph function '${currentOperation.getClass.getSimpleName}'."
    )
    currentOperation match {
      case PerspectiveDone()      =>
        logger.debug(
                s"Job '$jobID': Executing next perspective with windows '${currentPerspective.window}'" +
                  s" and timestamp '${currentPerspective.timestamp}'."
        )
        executeNextPerspective()

      case SetGlobalState(fun)    =>
        fun(graphState)
        nextGraphOperation(vertexCount)

      case f: GlobalGraphFunction =>
        messagetoAllJobWorkers(GraphFunctionWithGlobalState(f, graphState))
        if (f.isInstanceOf[TabularisingGraphFunction])
          Stages.ExecuteTable
        else
          Stages.ExecuteGraph

      case f: GraphFunction       =>
        messagetoAllJobWorkers(f)
        if (f.isInstanceOf[TabularisingGraphFunction])
          Stages.ExecuteTable
        else
          Stages.ExecuteGraph
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

  private def killJob(): Unit = {
    messagetoAllJobWorkers(EndQuery(jobID))
    workerList.close()
    querySupervisor.endQuery(jobID)
    logger.debug(s"Job '$jobID': No more perspectives available. Ending Query Handler execution.")
    tracker closeWithMessage JobDone
    finish.release
  }

  private def getNextGraphOperation(queue: mutable.Queue[GraphFunction]) =
    Try(queue.dequeue()).toOption

  private def getNextTableOperation(queue: mutable.Queue[TableFunction]) =
    Try(queue.dequeue()).toOption
}

object QueryHandler {

  def apply[F[_]: Async](
      querySupervisor: QuerySupervisor[F],
      scheduler: Scheduler,
      query: Query,
      conf: Config,
      topics: TopicRepository,
      earliestTime: Long,
      latestTime: Long
  ): F[Unit] =
    (for {
      dispatcher <- Dispatcher[F]
      finish     <- Resource.eval(cats.effect.std.Semaphore[F](0))
      _          <- QueryHandler[F](
                            querySupervisor,
                            scheduler,
                            query,
                            conf,
                            topics,
                            earliestTime,
                            latestTime,
                            dispatcher,
                            finish
                    )
    } yield finish).use(finish => finish.acquire)

  private def apply[F[_]: Async](
      querySupervisor: QuerySupervisor[F],
      scheduler: Scheduler,
      query: Query,
      conf: Config,
      topics: TopicRepository,
      earliestTime: Long,
      latestTime: Long,
      dispatcher: Dispatcher[F],
      finish: cats.effect.std.Semaphore[F]
  ): Resource[F, QueryHandler[F]] =
    Resource
      .make(
              for {
                handler <- Async[F].delay(
                                   new QueryHandler(
                                           query.graphID,
                                           querySupervisor,
                                           scheduler,
                                           query.name,
                                           query,
                                           conf,
                                           topics,
                                           query.pyScript,
                                           earliestTime,
                                           latestTime,
                                           dispatcher,
                                           finish
                                   )
                           )
                _       <- Async[F].blocking(handler.run())
              } yield handler
      )(handler => Async[F].blocking(handler.stop()))
}

private[raphtory] object Stages extends Enumeration {
  type Stage = Value
  val SpawnExecutors, EstablishPerspective, ExecuteGraph, ExecuteTable, EndTask = Value
}
