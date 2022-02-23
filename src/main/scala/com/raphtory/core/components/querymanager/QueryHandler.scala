package com.raphtory.core.components.querymanager

import com.raphtory.core.graph.PerspectiveController.DEFAULT_PERSPECTIVE_TIME
import com.raphtory.core.graph.PerspectiveController.DEFAULT_PERSPECTIVE_WINDOW
import Stages.SpawnExecutors
import Stages.Stage
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.algorithm._
import com.raphtory.core.components.Component
import com.raphtory.core.config.AsyncConsumer
import com.raphtory.core.config.MonixScheduler
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.Perspective
import com.raphtory.core.graph.PerspectiveController
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema

import java.util.concurrent.TimeUnit

abstract class QueryHandler(
    queryManager: QueryManager,
    scheduler: Scheduler,
    jobID: String,
    algorithm: GraphAlgorithm,
    outputFormat: OutputFormat,
    conf: Config,
    pulsarController: PulsarController
) extends Component[Array[Byte]](
                conf: Config,
                pulsarController: PulsarController,
                scheduler: Scheduler
        ) {

  private val self: Producer[Array[Byte]]                 = toQueryHandlerProducer(jobID)
  private val readers: Producer[Array[Byte]]              = toReaderProducer
  private val tracker: Producer[Array[Byte]]              = toQueryTrackerProducer(jobID)
  private val workerList: Map[Int, Producer[Array[Byte]]] = toQueryExecutorProducers(jobID)

  private var perspectiveController: PerspectiveController = _
  private var graphPerspective: GenericGraphPerspective    = _
  private var table: GenericTable                          = _
  private var currentOperation: GraphFunction              = _

  private var currentPerspective: Perspective =
    Perspective(DEFAULT_PERSPECTIVE_TIME, DEFAULT_PERSPECTIVE_WINDOW)

  private var lastTime: Long            = 0L
  private var readyCount: Int           = 0
  private var vertexCount: Int          = 0
  private var receivedMessageCount: Int = 0
  private var sentMessageCount: Int     = 0
  private var allVoteToHalt: Boolean    = true

  protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController

  private var currentState: Stage = SpawnExecutors

  class RecheckTimer extends Runnable {

    def run() {
      self sendAsync serialise(RecheckTime)
    }
  }
  private val recheckTimer = new RecheckTimer()

  override val consumer = Some(startQueryHandlerConsumer(Schema.BYTES, jobID))

  override def run(): Unit = {
    readers sendAsync serialise(EstablishExecutor(jobID))
    scheduler.execute(AsyncConsumer(this))
    logger.debug(s"Job '$jobID': Starting query handler consumer.")
  }

  override def stop(): Unit = {
    //consumer.close()
    self.close()
    readers.close()
    tracker.close()
    workerList.foreach(_._2.close())
  }

  override def handleMessage(msg: Message[Array[Byte]]): Boolean = {
    var scheduleAgain = true
    currentState match {
      case Stages.SpawnExecutors       => currentState = spawnExecutors(msg)
      case Stages.EstablishPerspective => currentState = establishPerspective(msg)
      case Stages.ExecuteGraph         => currentState = executeGraph(msg)
      case Stages.ExecuteTable         => currentState = executeTable(msg)
      case Stages.EndTask              => scheduleAgain = false
    }
    scheduleAgain
  }

  ////OPERATION STATES
  //Communicate with all readers and get them to spawn a QueryExecutor for their partition
  private def spawnExecutors(msg: Message[Array[Byte]]): Stage = {
    val workerID = deserialise[ExecutorEstablished](msg.getValue).worker
    logger.debug(s"Job '$jobID': Deserialized worker '$workerID'.")

    if (readyCount + 1 == totalPartitions) {
      val latestTime = whatsTheTime()
      perspectiveController = buildPerspectiveController(latestTime)
      logger.debug(s"Job '$jobID': Built perspective controller $perspectiveController.")

      readyCount = 0
      executeNextPerspective()
    }
    else {
      logger.debug(s"Job '$jobID': Spawning executors.")

      readyCount += 1
      Stages.SpawnExecutors
    }
  }

  //build the perspective within the QueryExecutor for each partition -- the view to be analysed
  private def establishPerspective(msg: Message[Array[Byte]]): Stage =
    deserialise[QueryManagement](msg.getValue) match {
      case RecheckTime               =>
        logger.debug(s"Job '$jobID': Rechecking time of $currentPerspective.")
        recheckTime(currentPerspective)

      case p: PerspectiveEstablished =>
        if (readyCount + 1 == totalPartitions) {
          vertexCount += p.vertices
          readyCount = 0
          messagetoAllJobWorkers(SetMetaData(vertexCount))
          logger.debug(s"Job '$jobID': Message to all workers vertex count: $vertexCount.")
          Stages.EstablishPerspective
        }
        else {
          vertexCount += p.vertices
          readyCount += 1
          Stages.EstablishPerspective
        }

      case MetaDataSet               =>
        if (readyCount + 1 == totalPartitions) {
          self sendAsync serialise(StartGraph)
          readyCount = 0
          receivedMessageCount = 0
          sentMessageCount = 0
          currentOperation = null
          allVoteToHalt = true

          logger.debug(
                  s"Job '$jobID': Executing graph with windows '${currentPerspective.window}' " +
                    s"at timestamp '${currentPerspective.timestamp}'."
          )

          Stages.ExecuteGraph
        }
        else {
          readyCount += 1
          Stages.EstablishPerspective
        }
    }

  //execute the steps of the graph algorithm until a select is run
  private def executeGraph(msg: Message[Array[Byte]]): Stage =
    deserialise[QueryManagement](msg.getValue) match {
      case StartGraph                                                         =>
        graphPerspective = new GenericGraphPerspective(vertexCount)

        logger.debug(s"Job '$jobID': Running '${algorithm.getClass.getSimpleName}'.")
        algorithm.run(graphPerspective)

        logger.debug(s"Job '$jobID': Writing results to '${outputFormat.getClass.getSimpleName}'.")
        table = graphPerspective.getTable()
        table.writeTo(outputFormat) //sets output formatter

        graphPerspective.getNextOperation() match {
          case Some(f: Select)        =>
            messagetoAllJobWorkers(f)
            readyCount = 0
            logger.debug(s"Job '$jobID': Executing Select function.")
            Stages.ExecuteTable

          case Some(f: GraphFunction) =>
            messagetoAllJobWorkers(f)
            currentOperation = f
            logger.debug(s"Job '$jobID': Executing '${algorithm.getClass.getSimpleName}' function.")
            readyCount = 0
            receivedMessageCount = 0
            sentMessageCount = 0
            allVoteToHalt = true
            Stages.ExecuteGraph
          case None                   =>
            logger.debug(s"Job '$jobID': Ending Task.")
            Stages.EndTask
        }

      case GraphFunctionComplete(receivedMessages, sentMessages, votedToHalt) =>
        val totalSentMessages     = sentMessageCount + sentMessages
        val totalReceivedMessages = receivedMessageCount + receivedMessages
        if ((readyCount + 1) == totalPartitions)
          if (totalReceivedMessages == totalSentMessages)
            currentOperation match {
              case Iterate(f, iterations, executeMessagedOnly) =>
                if (iterations == 1 || (allVoteToHalt && votedToHalt)) {
                  logger.debug(
                          s"Job '$jobID': Starting next operation '${algorithm.getClass.getSimpleName}'."
                  )
                  nextGraphOperation(vertexCount)
                }
                else {
                  messagetoAllJobWorkers(Iterate(f, iterations - 1, executeMessagedOnly))
                  currentOperation = Iterate(f, iterations - 1, executeMessagedOnly)
                  readyCount = 0
                  receivedMessageCount = 0
                  sentMessageCount = 0
                  allVoteToHalt = true
                  logger.debug(
                          s"Job '$jobID': Executing '${algorithm.getClass.getSimpleName}' function."
                  )
                  Stages.ExecuteGraph
                }
              case _                                           =>
                nextGraphOperation(vertexCount)
            }
          else {
            messagetoAllJobWorkers(CheckMessages(jobID))
            logger.debug(
                    s"Job '$jobID': Received messages:$receivedMessages , Sent messages: $sentMessages."
            )
            readyCount = 0
            receivedMessageCount = 0
            sentMessageCount = 0
            Stages.ExecuteGraph
          }
        else {
          sentMessageCount = totalSentMessages
          receivedMessageCount = totalReceivedMessages
          readyCount += 1
          allVoteToHalt = votedToHalt & allVoteToHalt
          Stages.ExecuteGraph
        }
    }

  //once the select has been run, execute all of the table functions until we hit a writeTo
  private def executeTable(msg: Message[Array[Byte]]): Stage =
    deserialise[QueryManagement](msg.getValue) match {
      case TableBuilt            =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          readyCount = 0
          logger.debug(s"Job '$jobID': Executing next table operation.")
          nextTableOperation()
        }
        else
          Stages.ExecuteTable

      case TableFunctionComplete =>
        readyCount += 1
        if (readyCount == totalPartitions) {
          logger.debug(s"Job '$jobID': Running next table operation.")
          nextTableOperation()
        }
        else
          Stages.ExecuteTable
    }
  ////END OPERATION STATES

  ///HELPER FUNCTIONS
  private def executeNextPerspective(): Stage = {
    val latestTime = whatsTheTime()
    if (currentPerspective.timestamp != -1) //ignore initial placeholder
      tracker.sendAsync(serialise(currentPerspective))
    perspectiveController.nextPerspective() match {
      case Some(perspective) if perspective.timestamp <= latestTime =>
        logger.debug(s"Job '$jobID': Perspective '$perspective' is starting.")
        logTimeTaken(perspective)
        messagetoAllJobWorkers(CreatePerspective(perspective.timestamp, perspective.window))
        currentPerspective = perspective
        graphPerspective = null
        table = null
        Stages.EstablishPerspective
      case Some(perspective)                                        =>
        logger.debug(
                s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$latestTime'."
        )
        logTimeTaken(perspective)
        currentPerspective = perspective
        graphPerspective = null
        table = null
        scheduler.scheduleOnce(1, TimeUnit.SECONDS, recheckTimer)

        Stages.EstablishPerspective
      case None                                                     =>
        logger.debug(s"Job '$jobID': No more perspectives to run.")
        //log.info(s"no more perspectives to run for $jobID")
        killJob()
        Stages.EndTask
    }
  }

  private def logTimeTaken(perspective: Perspective) = {
    if (currentPerspective.timestamp != DEFAULT_PERSPECTIVE_TIME)
      currentPerspective.window match {
        case Some(window) =>
          logger.trace(
                  s"Job '$jobID': Perspective at Time '${currentPerspective.timestamp}' with " +
                    s"Window $window took ${System.currentTimeMillis() - lastTime} ms to run."
          )
        case None         =>
          logger.trace(
                  s"Job '$jobID': Perspective at Time '${currentPerspective.timestamp}' " +
                    s"took ${System.currentTimeMillis() - lastTime} ms to run. "
          )
      }
    lastTime = System.currentTimeMillis()
  }

  private def recheckTime(perspective: Perspective): Stage = {
    val time = whatsTheTime()
    if (perspective.timestamp <= time) {
      logger.debug(s"Job '$jobID': Created perspective at time $time.")

      messagetoAllJobWorkers(CreatePerspective(perspective.timestamp, perspective.window))
      Stages.EstablishPerspective
    }
    else {
      logger.debug(s"Job '$jobID': Perspective '$perspective' is not ready, currently at '$time'.")

      scheduler.scheduleOnce(1, TimeUnit.SECONDS, recheckTimer)
      Stages.EstablishPerspective
    }
  }

  private def nextGraphOperation(vertexCount: Int): Stage =
    graphPerspective.getNextOperation() match {
      case Some(f: Select)        =>
        logger.debug(s"Job '$jobID': Executing Select function.")
        messagetoAllJobWorkers(f)
        readyCount = 0
        Stages.ExecuteTable

      case Some(f: GraphFunction) =>
        messagetoAllJobWorkers(f)
        currentOperation = f
        logger.debug(s"Job '$jobID': Executing graph function .")
        readyCount = 0
        receivedMessageCount = 0
        sentMessageCount = 0
        allVoteToHalt = true
        Stages.ExecuteGraph

      case None                   =>
        readyCount = 0
        receivedMessageCount = 0
        sentMessageCount = 0
        logger.debug(
                s"Job '$jobID': Executing next perspective with windows '${currentPerspective.window}'" +
                  s" and timestamp '${currentPerspective.timestamp}'."
        )
        executeNextPerspective()
    }

  private def nextTableOperation(): Stage =
    table.getNextOperation() match {

      case Some(f: TableFunction) =>
        messagetoAllJobWorkers(f)
        readyCount = 0
        logger.debug(s"Job '$jobID': Executing table function.")

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

    //log.info(s"Job '$jobID': Has no more perspectives. Ending Query Handler execution.")
    //if(monitor!=null)monitor ! TaskFinished(true)
  }

  def whatsTheTime(): Long = queryManager.whatsTheTime()

}

object Stages extends Enumeration {
  type Stage = Value
  val SpawnExecutors, EstablishPerspective, ExecuteGraph, ExecuteTable, EndTask = Value
}
