package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.GraphLenses.GraphLens
import com.raphtory.core.analysis.API.GraphLenses.ViewLens
import com.raphtory.core.analysis.API.GraphLenses.WindowLens
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.API._
import com.raphtory.core.analysis.Algorithms.ConnectedComponents
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils
import com.twitter.util.Eval
import kamon.Kamon
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

class ReaderWorker(managerCountVal: Int, managerID: Int, workerId: Int, storage: EntityStorage) extends Actor with ActorLogging {

  implicit var managerCount: ManagerCount = ManagerCount(managerCountVal)
  val analyserMap: TrieMap[String,LoadExternalAnalyser] = TrieMap[String,LoadExternalAnalyser]()
  var receivedMessages    = new AtomicInteger(0)
  var tempProxy: GraphLens = _

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.readersWorkerTopic, self)

  val messagesReceived  = Kamon.counter("Messages Received").withTag("actor",s"Reader_$managerID").withTag("ID",workerId)


  override def preStart(): Unit =
    log.debug("ReaderWorker [{}] belonging to Reader [{}] is being started.", workerId, managerID)

  override def receive: Receive = {

    case req: UpdatedCounter      => processUpdatedCounterRequest(req)
    case req: TimeCheck           => processTimeCheckRequest(req)
    case req: CompileNewAnalyser  => processCompileNewAnalyserRequest(req)
    case req: Setup               => processSetupRequest(req)
    case req: SetupNewAnalyser    => processSetupNewAnalyserRequest(req)
    case req: CheckMessages       => processCheckMessagesRequest(req)
    case req: NextStep            => processNextStepRequest(req)
    case req: NextStepNewAnalyser => processNextStepNewAnalyserRequest(req)
    case req: Finish              => processFinishRequest(req)
    case req: FinishNewAnalyser    => processFinishNewAnalyserRequest(req)

    case req: VertexMessage       => handleVertexMessage(req)
    case x                        => log.warning("ReaderWorker [{}] belonging to Reader [{}] received unknown [{}] message.", x)
  }

  def processUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    managerCount = ManagerCount(req.newValue)
  }

  def processSetupRequest(req: Setup): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    val superstepTimer = Kamon.timer("Raphtory_Superstep_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerId)
      .withTag("JobID",req.jobID)
      .withTag("timestamp",req.timestamp)
      .withTag("stage","setup")
      .start()
    try setup(req.analyzer, req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet)
    catch { case e: Exception => log.error("Failed to run setup due to [{}].", e) }
    superstepTimer.stop()
  }

  def processCheckMessagesRequest(req: CheckMessages): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    sender ! MessagesReceived(workerId, receivedMessages.get, tempProxy.getMessages())
  }

  def processNextStepRequest(req: NextStep): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    val superstepTimer = Kamon.timer("Raphtory_Superstep_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerId)
      .withTag("JobID",req.jobID)
      .withTag("timestamp",req.timestamp)
      .withTag("stage","analysis")
      .start()
    try nextStep(req.analyzer, req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet)
    catch { case e: Exception => log.error("Failed to run nextStep due to [{}].", e) }
    superstepTimer.stop()
  }

  def processNextStepNewAnalyserRequest(req: NextStepNewAnalyser): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    nextStepNewAnalyser(req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet)
  }

  def processSetupNewAnalyserRequest(req: SetupNewAnalyser): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    setupNewAnalyser(req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet)
  }

  def processFinishNewAnalyserRequest(req: FinishNewAnalyser): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    finishNewAnalyser(req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet)
  }

  def processFinishRequest(req: Finish): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    val superstepTimer = Kamon.timer("Raphtory_Superstep_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerId)
      .withTag("JobID",req.jobID)
      .withTag("timestamp",req.timestamp)
      .withTag("stage","results")
      .start()
    try returnResults(req.analyzer, req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet)
    catch { case e: Exception => log.error("Failed to run returnResults due to [{}].", e) }
    superstepTimer.stop()
  }

  def handleVertexMessage(req: VertexMessage): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    messagesReceived.withTag("JobID",req.jobID).withTag("timestamp",req.superStep)
      .increment()//kamon
    receivedMessages.incrementAndGet()//actual counter
    storage.vertices(req.vertexID).multiQueue.receiveMessage(req.jobID,req.superStep,req.data)
  }

  def setup(analyzer: Analyser, jobID: String, args: Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) {
    receivedMessages.set(0)
    setProxy(jobID, superStep, timestamp, analysisType, window, windowSet)
    analyzer.sysSetup(context, managerCount, tempProxy, workerId)
    if (windowSet.isEmpty) {
      analyzer.setup()
      sender ! Ready(tempProxy.getMessages())
    }
    else {
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

  def nextStep(analyzer: Analyser,jobID: String,args: Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit = {
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

  def setupNewAnalyser(jobID: String, args: Array[String], currentStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit = {
    setup(analyserMap.get(jobID).get.newAnalyser, jobID, args, currentStep, timestamp, analysisType, window, windowSet)
  }
  def nextStepNewAnalyser(jobID: String, args: Array[String], currentStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit = {
    nextStep(analyserMap.get(jobID).get.newAnalyser, jobID, args, currentStep, timestamp, analysisType, window, windowSet)
  }

  def finishNewAnalyser(jobID: String, args: Array[String], currentStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit = {
    returnResults(analyserMap.get(jobID).get.newAnalyser, jobID, args, currentStep, timestamp, analysisType, window, windowSet)
  }

  def returnResults(analyzer: Analyser, jobID: String, args: Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit = {
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
    }
    else {
      log.debug("Received timestamp is larger than newest entityStorage timestamp.")
      sender ! TimeResponse(ok = false, newest)
    }
  }



  def processCompileNewAnalyserRequest(req: CompileNewAnalyser)= {
    try{
      val analyserBuilder = LoadExternalAnalyser(req.analyser,req.args)
      analyserMap put (req.name,analyserBuilder)
      analyserBuilder.newAnalyser
      sender() ! AnalyserPresent()
    }
    catch {
      case e:Exception => {
        sender ! FailedToCompile(e.getStackTrace.toString)
        println(e.getMessage)
      }
    }
  }

  private def setProxy(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit =
    analysisType match {
      case AnalysisType.live  => handleLiveAnalysis(jobID, superStep, timestamp, analysisType, window, windowSet)
      case AnalysisType.view  => handleViewAnalysis(jobID, superStep, timestamp, analysisType, window, windowSet)
      case AnalysisType.range => handleRangeAnalysis(jobID, superStep, timestamp, analysisType, window, windowSet)
    }

  def handleLiveAnalysis(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit =
    // We have a set of windows to run
    if (windowSet.nonEmpty)
      tempProxy = new WindowLens(jobID, superStep, timestamp, windowSet(0), workerId, storage, managerCount)
    // We only have one window to run
    else if (window != -1)
      tempProxy = new WindowLens(jobID, superStep, timestamp, window, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(jobID, superStep, timestamp, workerId, storage, managerCount)

  def handleViewAnalysis(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit =
    if (windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, windowSet(0), workerId, storage, managerCount)
    else if (window != -1) // we only have one window to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, window, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(jobID, superStep, timestamp, workerId, storage, managerCount)

  def handleRangeAnalysis(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit =
    if (windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, windowSet(0), workerId, storage, managerCount)
    else if (window != -1) // we only have one window to run
      tempProxy = new WindowLens(jobID, superStep, timestamp, window, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(jobID, superStep, timestamp, workerId, storage, managerCount)
}
