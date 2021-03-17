package com.raphtory.core.actors.PartitionManager.Workers

import java.util.concurrent.atomic.AtomicInteger
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.api.{Analyser, LoadExternalAnalyser, ManagerCount}
import com.raphtory.core.actors.ClusterManagement.RaphtoryReplicator.Message.UpdatedCounter
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.analysis.GraphLenses.{GraphLens, ViewLens, WindowLens}
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication._
import kamon.Kamon

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap

case class ViewJob(jobID:String,timestamp:Long,window:Long)

class ReaderWorker(managerCountVal: Int, managerID: Int, workerId: Int, storage: EntityStorage) extends RaphtoryActor {

  implicit var managerCount: ManagerCount = ManagerCount(managerCountVal)
  val analyserMap: TrieMap[String,LoadExternalAnalyser] = TrieMap[String,LoadExternalAnalyser]()
  private val sentMessageMap     = ParTrieMap[String, AtomicInteger ]()
  private val receivedMessageMap = ParTrieMap[String, AtomicInteger ]()
  var tempProxy: GraphLens = _

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

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
    case req: FinishNewAnalyser   => processFinishNewAnalyserRequest(req)

    case req: VertexMessage       => handleVertexMessage(req)
    case x                        => log.warning("ReaderWorker [{}] belonging to Reader [{}] received unknown [{}] message.", x)
  }

  def processUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)

    managerCount = ManagerCount(req.newValue)
  }

  def processSetupRequest(req: Setup): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    val beforeTime = System.currentTimeMillis()
    val superstepTimer = Kamon.gauge("Raphtory_Superstep_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerId)
      .withTag("JobID",req.jobID)
      .withTag("timestamp",req.timestamp)
      .withTag("superstep",req.superStep)
    try setup(req.analyzer, req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet.sortBy(x=>x)(sortOrdering))
    catch { case e: Exception => log.error("Failed to run setup due to [{}].", e.printStackTrace()) }
    superstepTimer.update(System.currentTimeMillis()-beforeTime)
  }

  def processCheckMessagesRequest(req: CheckMessages): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    Kamon.gauge("Raphtory_Analysis_Messages_Received")
      .withTag("actor",s"Reader_$managerID")
      .withTag("ID",workerId)
      .withTag("jobID",req.jobID.jobID)
      .withTag("Timestamp",req.jobID.timestamp)
      .withTag("Superstep",req.superstep)
      .update(getReceivedMessages(req.jobID.jobID))

    Kamon.gauge("Raphtory_Analysis_Messages_Sent")
      .withTag("actor",s"Reader_$managerID")
      .withTag("ID",workerId)
      .withTag("jobID",req.jobID.jobID)
      .withTag("Timestamp",req.jobID.timestamp)
      .withTag("Superstep",req.superstep)
      .update(getReceivedMessages(req.jobID.jobID))

    sender ! MessagesReceived(workerId, getReceivedMessages(req.jobID.jobID),getSentMessages(req.jobID.jobID))
  }

  def processNextStepRequest(req: NextStep): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    val beforeTime = System.currentTimeMillis()
    val superstepTimer = Kamon.gauge("Raphtory_Superstep_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerId)
      .withTag("JobID",req.jobID)
      .withTag("timestamp",req.timestamp)
      .withTag("superstep",req.superStep)

    try nextStep(req.analyzer, req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet.sortBy(x=>x)(sortOrdering))
    catch { case e: Exception => log.error("Failed to run nextStep due to [{}].", e) }
    superstepTimer.update(System.currentTimeMillis()-beforeTime)
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
    val beforeTime = System.currentTimeMillis()
    val superstepTimer = Kamon.gauge("Raphtory_Superstep_Time")
      .withTag("Partition",storage.managerID)
      .withTag("Worker",workerId)
      .withTag("JobID",req.jobID)
      .withTag("timestamp",req.timestamp)
      .withTag("superstep",req.superStep)

    try returnResults(req.analyzer, req.jobID, req.args, req.superStep, req.timestamp, req.analysisType, req.window, req.windowSet.sortBy(x=>x)(sortOrdering))
    catch { case e: Exception => log.error("Failed to run returnResults due to [{}].", e) }
    superstepTimer.update(System.currentTimeMillis()-beforeTime)
  }

  def handleVertexMessage(req: VertexMessage): Unit = {
    log.debug("ReaderWorker [{}] belonging to Reader [{}] received [{}] request.", managerID, workerId, req)
    incrementReceivedMessages(req.viewJob.jobID)
    storage.vertices(req.vertexID).multiQueue.receiveMessage(req.viewJob,req.superStep,req.data)
  }

  def setup(analyzer: Analyser, jobID: String, args: Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]) {

    setProxy(jobID, superStep, timestamp, analysisType, window, windowSet)
    analyzer.sysSetup(context, managerCount, tempProxy, workerId)

    if (windowSet.isEmpty) {
      analyzer.setup()
      incrementSentMessages(jobID,tempProxy.getMessages())
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
      incrementSentMessages(jobID,tempProxy.getMessages())
      sender ! Ready(tempProxy.getMessages())
    }
  }

  def nextStep(analyzer: Analyser,jobID: String,args: Array[String], superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit = {

    setProxy(jobID, superStep, timestamp, analysisType, window, windowSet)
    analyzer.sysSetup(context, managerCount, tempProxy, workerId)
    if (windowSet.isEmpty) {
      analyzer.analyse()
      incrementSentMessages(jobID,tempProxy.getMessages())
      sender ! EndStep(tempProxy.getMessages(), tempProxy.checkVotes(workerId))
    } else {
      analyzer.analyse()

      for (i <- windowSet.indices)
        if (i != 0) {
          tempProxy.asInstanceOf[WindowLens].shrinkWindow(windowSet(i))
          analyzer.analyse()
        }
      incrementSentMessages(jobID,tempProxy.getMessages())
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
      //storage.vertices.foreach(v=>v._2.computationValues = ParTrieMap[String, Any]())
    }
  }

  def processTimeCheckRequest(req: TimeCheck): Unit = {
    log.debug(s"Reader [{}] received [{}] request.", workerId, req)
    val timestamp = req.timestamp
    val newest = storage.windowTime

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
      tempProxy = new WindowLens(ViewJob(jobID,timestamp,windowSet(0)), superStep,workerId, storage, managerCount)
    // We only have one window to run
    else if (window != -1)
      tempProxy = new WindowLens(ViewJob(jobID,timestamp,window), superStep, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(ViewJob(jobID,timestamp,-1), superStep, workerId, storage, managerCount)

  def handleViewAnalysis(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit =
    if (windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowLens(ViewJob(jobID,timestamp,windowSet(0)), superStep,  workerId, storage, managerCount)
    else if (window != -1) // we only have one window to run
      tempProxy = new WindowLens(ViewJob(jobID,timestamp,window), superStep, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(ViewJob(jobID,timestamp,-1), superStep, workerId, storage, managerCount)

  def handleRangeAnalysis(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long]): Unit =
    if (windowSet.nonEmpty) //we have a set of windows to run
      tempProxy = new WindowLens(ViewJob(jobID,timestamp,windowSet(0)), superStep, workerId, storage, managerCount)
    else if (window != -1) // we only have one window to run
      tempProxy = new WindowLens(ViewJob(jobID,timestamp,window), superStep, workerId, storage, managerCount)
    else
      tempProxy = new ViewLens(ViewJob(jobID,timestamp,-1), superStep, workerId, storage, managerCount)


  def incrementReceivedMessages(jobID:String) = {
    receivedMessageMap.get(jobID) match {
      case Some(counter) => counter.incrementAndGet()
      case None => receivedMessageMap.put(jobID,new AtomicInteger(1))
    }
  }

  def getReceivedMessages(jobID:String) = {
    receivedMessageMap.get(jobID) match {
      case Some(counter) => counter.get()
      case None => 0
    }
  }

  def incrementSentMessages(jobID:String, value:Int) = {
    sentMessageMap.get(jobID) match {
      case Some(counter) => counter.set(counter.get()+value)//actual counter
      case None => sentMessageMap.put(jobID,new AtomicInteger(value))
    }
  }

  def getSentMessages(jobID:String) = {
    sentMessageMap.get(jobID) match {
      case Some(counter) => counter.get
      case None => 0
    }
  }



}
