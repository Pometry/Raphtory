package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.GraphLenses.LiveLens
import com.raphtory.core.analysis.API.GraphLenses.ViewLens
import com.raphtory.core.analysis.API.GraphLenses.WindowLens
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API._
import com.raphtory.core.analysis.Algorithms.ConnectedComponents
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval
import monix.execution.atomic.AtomicInt

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class ReaderWorker(managerCountVal: Int, managerID: Int, workerId: Int, storage: EntityStorage)
        extends Actor
        with ActorLogging {

  implicit var managerCount: ManagerCount = ManagerCount(managerCountVal)

  var receivedMessages    = AtomicInt(0)
  var tempProxy: LiveLens = _

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)

  override def preStart(): Unit =
    log.debug("ReaderWorker [{}] belonging to Reader [{}] is being started.", workerId, managerID)

  override def receive: Receive = {

    case req: UpdatedCounter      => processUpdatedCounterRequest(req)
    case req: TimeCheck            => processTimeCheckRequest(req)
    case req: CompileNewAnalyser   => processCompileNewAnalyserRequest(req)
    case req: Setup               => processSetupRequest(req)
    case req: CheckMessages       => processCheckMessagesRequest(req)
    case req: NextStep            => processNextStepRequest(req)
    case req: NextStepNewAnalyser => processNextStepNewAnalyserRequest(req)
    case req: Finish              => processFinishRequest(req)
    case req: VertexMessage       => handleVertexMessage(req)
    case x                        => log.warning("ReaderWorker [{}] belonging to Reader [{}] received unknown [{}] message.", x)
  }

  def processUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    managerCount = ManagerCount(req.newValue)
  }

  def processSetupRequest(req: Setup): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    try setup(
            req.analyzer,
            req.jobID,
            req.args,
            req.superStep,
            req.timestamp,
            req.analysisType,
            req.window,
            req.windowSet
    )
    catch { case e: Exception => log.error("Failed to run setup due to [{}].", e) }
  }

  def processCheckMessagesRequest(req: CheckMessages): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    var count = AtomicInt(0)

    //tempProxy.getVerticesSet().foreach(v => count.add(tempProxy.getVertex(v._2).messageQueue2.size))

    sender ! MessagesReceived(workerId, count.get, receivedMessages.get, tempProxy.getMessages())
  }

  def processNextStepRequest(req: NextStep): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    try nextStep(
            req.analyzer,
            req.jobID,
            req.args,
            req.superStep,
            req.timestamp,
            req.analysisType,
            req.window,
            req.windowSet
    )
    catch { case e: Exception => log.error("Failed to run nextStep due to [{}].", e) }
  }

  def processNextStepNewAnalyserRequest(req: NextStepNewAnalyser): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    nextStepNewAnalyser(
            req.name,
            req.jobID,
            req.args,
            req.superStep,
            req.timestamp,
            req.analysisType,
            req.window,
            req.windowSet
    )
  }

  def processFinishRequest(req: Finish): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    try returnResults(
            req.analyzer,
            req.jobID,
            req.args,
            req.superStep,
            req.timestamp,
            req.analysisType,
            req.window,
            req.windowSet
    )
    catch { case e: Exception => log.error("Failed to run returnResults due to [{}].", e) }
  }

  def handleVertexMessage(req: VertexMessage): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    req match {
      case VertexMessageFloat(_, vertexID, jobID, superStep, data) =>
        processVertexMessage(vertexID, jobID, superStep, data)
      case VertexMessageInt(_: Long, vertexID: Long, jobID: String, superStep: Int, data: Int) =>
        processVertexMessage(vertexID, jobID, superStep, data)
      case VertexMessageLong(_: Long, vertexID: Long, jobID: String, superStep: Int, data: Long) =>
        processVertexMessage(vertexID, jobID, superStep, data)
      case VertexMessageStringLong(_: Long, vertexID: Long, jobID: String, superStep: Int, data: (String, Long)) =>
        processVertexMessage(vertexID, jobID, superStep, data)
      case VertexMessageBatch(jobID: String, superStep: Int, batchData: Set[(Long, Long, Any)]) =>
        batchData.foreach { case (vertexID, _, data) => processVertexMessage(vertexID, jobID, superStep, data) }
    }
  }

  def processVertexMessage(vertexID: Long, jobID: String, superStep: Int, data: Any): ArrayBuffer[Any] = {
    receivedMessages.increment()
    storage.vertices(vertexID).multiQueue.receiveMessage(jobID, superStep, data)
  }

  def setup(
      analyzer: Analyser,
      jobID: String,
      args: Array[String],
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ) {
    receivedMessages.set(0)

    setProxy(jobID, superStep, timestamp, analysisType, window, windowSet)
    analyzer.sysSetup(context, managerCount, tempProxy, workerId)
    if (windowSet.isEmpty) {
      analyzer.setup()
      sender ! Ready(tempProxy.getMessages())
    } else {
      analyzer.setup()
      var currentWindow = 1

      while (currentWindow < windowSet.length) {
        tempProxy.asInstanceOf[WindowLens].shrinkWindow(windowSet(currentWindow))
        analyzer.setup()
        currentWindow += 1
      }

      sender ! Ready(tempProxy.getMessages())
    }
  }

  def nextStep(
      analyzer: Analyser,
      jobID: String,
      args: Array[String],
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit = {
    receivedMessages.set(0)

    setProxy(jobID, superStep, timestamp, analysisType, window, windowSet)
    analyzer.sysSetup(context, managerCount, tempProxy, workerId)
    if (windowSet.isEmpty) {
      analyzer.analyse()
      sender ! EndStep(tempProxy.getMessages(), tempProxy.checkVotes(workerId))
    } else {
      analyzer.analyse()

      for (i <- windowSet.indices)
        if (i != 0) {
          tempProxy.asInstanceOf[WindowLens].shrinkWindow(windowSet(i))
          analyzer.analyse()
        }

      sender ! EndStep(tempProxy.getMessages(), tempProxy.checkVotes(workerId))
    }

  }

  def nextStepNewAnalyser(
      name: String,
      jobID: String,
      args: Array[String],
      currentStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit = nextStep(new ConnectedComponents(Array()), jobID, args, currentStep, timestamp, analysisType, window, windowSet)

  def returnResults(
      analyzer: Analyser,
      jobID: String,
      args: Array[String],
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit = {
    setProxy(jobID, superStep, timestamp, analysisType, window, windowSet)
    analyzer.sysSetup(context, managerCount, tempProxy, workerId)
    if (windowSet.isEmpty) {
      val result = analyzer.returnResults()
      sender ! ReturnResults(result)
    } else {
      val individualResults: mutable.ArrayBuffer[Any] = ArrayBuffer[Any]()
      individualResults += analyzer.returnResults()
      for (i <- windowSet.indices)
        if (i != 0) {
          tempProxy.asInstanceOf[WindowLens].shrinkWindow(windowSet(i))
          individualResults += analyzer.returnResults()
        }
      sender ! ReturnResults(individualResults)
    }
  }

  def processTimeCheckRequest(req: TimeCheck): Unit = {
    log.debug(s"Reader [{}] received [{}] request.", workerId, req)

    val timestamp = req.timestamp

    val newest = if(storage.windowSafe) storage.safeWindowTime else storage.windowTime

    if (timestamp <= newest) {
      log.debug("Received timestamp is smaller or equal to newest entityStorage timestamp.")

      sender ! TimeResponse(ok = true, newest)
    } else {
      log.debug("Received timestamp is larger than newest entityStorage timestamp.")

      sender ! TimeResponse(ok = false, newest)
    }
  }


  def processCompileNewAnalyserRequest(req: CompileNewAnalyser): Unit = {
    log.debug("Reader [{}] received [{}] request.", workerId, req)

    val (analyserString, name) = (req.analyser, req.name)

    log.debug("Compiling [{}] for LAM.", name)

    val evalResult = Try {
      val eval               = new Eval
      val analyser: Analyser = eval[Analyser](analyserString)
      //Utils.analyserMap += ((name, analyser))
    }

    evalResult.toEither.fold(
      { t: Throwable =>
        log.debug("Compilation of [{}] failed due to [{}].", name, t)

        sender ! ClassMissing()
      }, { _ =>
        log.debug(s"Compilation of [{}] succeeded. Proceeding.", name)

        sender ! ClassCompiled()
      }
    )
  }


  private def setProxy(
      jobID: String,
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit =
    analysisType match {
      case AnalysisType.live  => handleLiveAnalysis(jobID, superStep, timestamp, analysisType, window, windowSet)
      case AnalysisType.view  => handleViewAnalysis(jobID, superStep, timestamp, analysisType, window, windowSet)
      case AnalysisType.range => handleRangeAnalysis(jobID, superStep, timestamp, analysisType, window, windowSet)
    }

  def handleLiveAnalysis(
      jobID: String,
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit =
    // We have a set of windows to run
    if (windowSet.nonEmpty)
      tempProxy = new WindowLens(jobID, superStep, storage.newestTime, windowSet(0), workerId, storage, managerCount)
    // We only have one window to run
    else if (window != -1)
      tempProxy = new WindowLens(jobID, superStep, storage.newestTime, window, workerId, storage, managerCount)
    else
      tempProxy = new LiveLens(jobID, superStep, timestamp, window, workerId, storage, managerCount)

  def handleViewAnalysis(
      jobID: String,
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit =
    if (windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, windowSet(0), workerId, storage, managerCount)
    else if (window != -1) // we only have one window to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, window, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(jobID, superStep, timestamp, workerId, storage, managerCount)

  def handleRangeAnalysis(
      jobID: String,
      superStep: Int,
      timestamp: Long,
      analysisType: AnalysisType.Value,
      window: Long,
      windowSet: Array[Long]
  ): Unit =
    if (windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, windowSet(0), workerId, storage, managerCount)
    else if (window != -1) // we only have one window to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, window, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(jobID, superStep, timestamp, workerId, storage, managerCount)
}
