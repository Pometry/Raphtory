package com.raphtory.core.actors.AnalysisManager.Tasks

import java.net.InetAddress
import akka.actor.{Actor, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.mongodb.DBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.util.JSON
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.Message._
import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask.Message._
import com.raphtory.core.actors.PartitionManager.Workers.ViewJob
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication._
import kamon.Kamon

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.reflect.io.Path

abstract class AnalysisTask(jobID: String, args:Array[String], analyser: Analyser[Any], managerCount:Int,newAnalyser: Boolean,rawFile:String) extends RaphtoryActor {
  protected var currentSuperStep    = 0 //SuperStep the algorithm is currently on
  implicit val executionContext = context.system.dispatchers.lookup("analysis-dispatcher")

  val saveData  = System.getenv().getOrDefault("ANALYSIS_SAVE_OUTPUT", "false").trim.toBoolean
  val mongoIP   = System.getenv().getOrDefault("ANALYSIS_MONGO_HOST", "localhost").trim
  val mongoPort = System.getenv().getOrDefault("ANALYSIS_MONGO_PORT", "27017").trim
  val dbname    = System.getenv().getOrDefault("ANALYSIS_MONGO_DB_NAME", "raphtory").trim

  val mongo = if(saveData) MongoClient(MongoClientURI(s"mongodb://${InetAddress.getByName(mongoIP).getHostAddress()}:$mongoPort")) else null


  var viewTimeCurrentTimer = System.currentTimeMillis()

  //Communication Counters
  protected var ReaderACKS                   = 0    //Acks from the readers to say they are online
  protected var ReaderAnalyserACKS           = 0    //Ack to show that the chosen analyser is present within the partition
  protected var TimeOKFlag                   = true //flag to specify if the job is ok to run if temporal
  protected var TimeOKACKS                   = 0    //counter to see that all partitions have responded
  protected var workersFinishedSetup         = 0    //Acks from workers to see how many have finished the setup stage of analysis
  protected var workersFinishedSuperStep     = 0    //Acks from workers to say they have finished the superstep
  protected var workerResultsReceived        = 0    //Acks from workers when sending results back to analysis manager
  protected var messageLogACKS               = 0    //Acks from Workers with the amount of messages they have sent and received
  protected var recentlySeenTime             = Long.MaxValue //tracking the earliest time seen

  private var totalReceivedMessages = 0 //Total number of messages received by the workers
  private var totalSentMessages     = 0 //Total number of messages sent by the workers

  private var voteToHalt = true // If the workers have decided to halt

  private val debug                = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean //for printing debug messages

  protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  private var results: ArrayBuffer[Any] = ArrayBuffer[Any]()

  def result() = results.toArray

  protected def analysisType(): AnalysisType.Value

  private var lastTime = System.currentTimeMillis()
  def viewCompleteTime(): Long = {
    val newTime   = System.currentTimeMillis()
    val totalTime = newTime - lastTime
    lastTime = newTime
    totalTime
  }

  private var timetakenpersuperstep = System.currentTimeMillis()
  def stepCompleteTime(): Long = {
    val newTime   = System.currentTimeMillis()
    val totalTime = newTime - timetakenpersuperstep
    timetakenpersuperstep = newTime
    totalTime
  }
  viewCompleteTime()
  stepCompleteTime()

  protected val steps: Long    =  analyser.defineMaxSteps()         // number of supersteps before returning
  def restartTime():Long       =  0
  def timestamp(): Long   //defaults to live to be overwritten in view and range
  def windowSize(): Long       = -1L         //for windowing
  def windowSet(): Array[Long] = Array.empty //for sets of windows



  private def processOtherMessages(value: Any): Unit = println("Not handled message" + value.toString)
  protected def generateAnalyzer: Analyser[Any] =
    try{
      Class.forName(analyser.getClass.getCanonicalName).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser[Any]]
    }
    catch {
      case e:NoSuchMethodException => Class.forName(analyser.getClass.getCanonicalName).getConstructor().newInstance().asInstanceOf[Analyser[Any]]
    }

  final protected def getManagerCount: Int      = managerCount
  final protected def getWorkerCount: Int       = managerCount * totalWorkers

  protected def processResults(timeStamp:Long) = {
    analyser.extractResults(results.toArray)
  }

  protected def processResultsWrapper(timeStamp: Long) = {
    try{
      if(saveData) {
        val buffer = new java.util.ArrayList[DBObject]()
        processResults(timeStamp)
        analyser.getPublishedData().foreach(data => {
          buffer.add(JSON.parse(data).asInstanceOf[DBObject])
        })
        analyser.clearPublishedData()
        if(!buffer.isEmpty){
          mongo.getDB(dbname).getCollection(jobID).insert(buffer)
        }
        //mongo.
      }
      else {
        analyser.clearPublishedData()
        processResults(timeStamp)
      }
    }
    catch {
      case e:Exception => e.printStackTrace()
    }



  }

  protected def resetCounters() = {
    ReaderACKS = 0               //Acks from the readers to say they are online
    ReaderAnalyserACKS = 0       //Ack to show that the chosen analyser is present within the partition
    workersFinishedSetup = 0     //Acks from workers to see how many have finished the setup stage of analysis
    workersFinishedSuperStep = 0 //Acks from workers to say they have finished the superstep
    messageLogACKS = 0           //Acks from Workers with the amount of messages they have sent and received
    totalReceivedMessages = 0    //Total number of messages received by the workers
    totalSentMessages = 0        //Total number of messages sent by the workers
    workerResultsReceived = 0    //Total number of acks from worker when they are sending results back
    currentSuperStep = 0
    results = ArrayBuffer[Any]()
  }

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(partitionsTopic, self)

  override def preStart() {
    context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis)
  }

  override def receive: Receive = {
    case StartAnalysis               => startAnalysis() //received message to start from Orchestrator
    case ReaderWorkersAck           => readerACK() //count up number of acks and if == number of workers, check if analyser present
    case AnalyserPresent             => analyserPresent() //analyser confirmed to be present within workers, send setup request to workers
    case TimeResponse(ok, time)        => timeResponse(ok, time) //checking if the timestamps are ok within all partitions
    case "recheckTime"                 => timeRecheck() //if the time was previous out of scope, wait and then recheck
    case Ready(messagesSent)           => ready(messagesSent) //worker has completed setup and is ready to roll -- send nextstep
    case EndStep(messages, voteToHalt) => endStep(messages, voteToHalt) //worker has finished the step
    case ReturnResults(results)        => finaliseJob(results) //worker has finished teh job and is returned the results
    case "restart"                     => restart()
    case MessagesReceived(workerID, receivedMessages, sentMessages) => messagesReceieved(workerID, receivedMessages, sentMessages)
    case RequestResults(_)         => requestResults()
    case FailedToCompile(stackTrace)   => failedToCompile(stackTrace) //Your code is broke scrub
    case _                             => processOtherMessages(_)     //incase some random stuff comes through
  }

  def requestResults() =sender() ! ResultsForApiPI(analyser.getPublishedData())

  def startAnalysis() = {
    for (worker <- getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, ReaderWorkersOnline, false)
  }

  def readerACK() = {
    if (debug) println("Received NetworkSize packet")
    ReaderACKS += 1
    if (ReaderACKS == getManagerCount) {
      if (newAnalyser) {
        for (worker <- getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator
            .Send(worker, CompileNewAnalyser(rawFile,args,jobID), false)
      }
      else {
        for (worker <- getAllReaders(managerCount))
          mediator ! DistributedPubSubMediator
            .Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$", "")), false)
      }
    }
  }

  def analyserPresent() = {
    ReaderAnalyserACKS += 1
    if(newAnalyser){
      if (ReaderAnalyserACKS == getWorkerCount) {
        for (worker <- getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
      }
    }
    else{
      if (ReaderAnalyserACKS == getManagerCount) {
        for (worker <- getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
      }
    }

  }

  def timeResponse(ok: Boolean, time: Long) = {
    if (!ok)
      TimeOKFlag = false
    TimeOKACKS += 1
    if(time<recentlySeenTime)recentlySeenTime=time
    if (TimeOKACKS == getWorkerCount) {
      stepCompleteTime() //reset step counter
      if (TimeOKFlag) {
        viewTimeCurrentTimer = System.currentTimeMillis()
        if (analyser.defineMaxSteps() > 1)
          for (worker <- getAllReaderWorkers(managerCount))
            if(newAnalyser)
              mediator ! DistributedPubSubMediator.Send(worker, SetupNewAnalyser(jobID, currentSuperStep, timestamp, analysisType(), windowSize(), windowSet()),false)
            else
              mediator ! DistributedPubSubMediator.Send(worker, Setup(this.generateAnalyzer, jobID, currentSuperStep, timestamp, analysisType(), windowSize(), windowSet()),false)
        else
          for (worker <- getAllReaderWorkers(managerCount))
            if(newAnalyser)
              mediator ! DistributedPubSubMediator.Send(worker, FinishNewAnalyser(jobID, currentSuperStep, timestamp, analysisType(), windowSize(), windowSet()), false)
            else
              mediator ! DistributedPubSubMediator.Send(worker, Finish(this.generateAnalyzer, jobID, currentSuperStep, timestamp, analysisType(), windowSize(), windowSet()), false)
      }
      else {
        handleTimeOutput()
      }

      TimeOKACKS = 0
      TimeOKFlag = true
    }
  }

  def timeRecheck() = {
    for (worker <- getAllReaderWorkers(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
  }

  def ready(messages: Int) = {
    if (debug) println("Received ready")
    workersFinishedSetup += 1
    totalSentMessages += messages
    if (debug) println(s"$workersFinishedSetup / $getWorkerCount")
    if (workersFinishedSetup == getWorkerCount) {
      workersFinishedSetup = 0
      syncMessages()
    }
  }

  def endStep(messages: Int, voteToHalt: Boolean) = {
    workersFinishedSuperStep += 1
    totalSentMessages += messages
    if (!voteToHalt) this.voteToHalt = false
    if (debug) println(s"$workersFinishedSuperStep / $getWorkerCount : $currentSuperStep / $steps")
    if (workersFinishedSuperStep == getWorkerCount)
      if (currentSuperStep == steps || this.voteToHalt)
        for (worker <- getAllReaderWorkers(managerCount))
          if(newAnalyser)
            mediator ! DistributedPubSubMediator.Send(worker, FinishNewAnalyser(jobID, currentSuperStep, timestamp, analysisType(), windowSize(), windowSet()), false)
          else
            mediator ! DistributedPubSubMediator.Send(worker, Finish(this.generateAnalyzer, jobID, currentSuperStep, timestamp, analysisType(), windowSize(), windowSet()), false)
      else {
        //println(s"Superstep $currentSuperStep")
        this.voteToHalt = true
        workersFinishedSuperStep = 0
        syncMessages()
      }
//    println(currentSuperStep,totalSentMessages)
  }

  def finaliseJob(result: Any) = {
    results = results += result
    workerResultsReceived += 1
    if (workerResultsReceived == getWorkerCount) {
      val startTime = System.currentTimeMillis()
      val viewTime = Kamon.gauge("Raphtory_View_Time_Total").withTag("jobID",jobID).withTag("Timestamp",timestamp())
      val concatTime = Kamon.gauge("Raphtory_View_Concatenation_Time").withTag("jobID",jobID).withTag("Timestamp",timestamp())
      processResultsWrapper(timestamp())
      resetCounters()
      context.system.scheduler.scheduleOnce(Duration(restartTime(), MILLISECONDS), self, "restart")
      concatTime.update(System.currentTimeMillis()-startTime)
      viewTime.update(System.currentTimeMillis()-viewTimeCurrentTimer)
    }

  }

  private def syncMessages() =
    if (totalSentMessages == 0) {
      currentSuperStep += 1
      if (newAnalyser)
        for (worker <- getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, NextStepNewAnalyser(jobID, currentSuperStep, timestamp, analysisType: AnalysisType.Value, windowSize(), windowSet()), false)
      else
        for (worker <- getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, NextStep(this.generateAnalyzer, jobID, currentSuperStep, timestamp, analysisType: AnalysisType.Value, windowSize(), windowSet()), false)
    }
    else {
      totalSentMessages = 0
      totalReceivedMessages = 0
      for (worker <- getAllReaderWorkers(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(ViewJob(jobID,timestamp(),-1),currentSuperStep), false)
    }

  def messagesReceieved(workerID: Int, receivedMessages: Int, sentMessages: Int) = {
//    println(s"+++ Superstep: $currentSuperStep  wID: $workerID    Messages in: $totalReceivedMessages    Messages out: $totalSentMessages ")
//      println(totalReceivedMessages)
    messageLogACKS += 1
    totalReceivedMessages += receivedMessages
    totalSentMessages += sentMessages
    if (messageLogACKS == getWorkerCount) {
      messageLogACKS = 0
      if (totalReceivedMessages == totalSentMessages) {
        currentSuperStep += 1
        totalSentMessages = 0
        totalReceivedMessages = 0
        if (newAnalyser)
          for (worker <- getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, NextStepNewAnalyser(jobID, currentSuperStep, timestamp, analysisType: AnalysisType.Value, windowSize(), windowSet()), false)
        else
          for (worker <- getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, NextStep(this.generateAnalyzer, jobID, currentSuperStep, timestamp, analysisType: AnalysisType.Value, windowSize(), windowSet()), false)
      }
      else {
        totalReceivedMessages = 0
        totalSentMessages = 0
        if (newAnalyser)
          for (worker <- getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(ViewJob(jobID,timestamp(),-1),currentSuperStep),false)
        else
          for (worker <- getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(ViewJob(jobID,timestamp(),-1),currentSuperStep),false)
      }
    }
  }

  def handleTimeOutput() = {
    println(s"${timestamp()} is yet to be ingested, currently at ${recentlySeenTime}. Retrying analysis in 10 seconds and retrying")
    context.system.scheduler.scheduleOnce(Duration(10000, MILLISECONDS), self, "recheckTime")
    recentlySeenTime = Long.MaxValue

  }

  def restart()

  def partitionsHalting() = voteToHalt

  def killme() = self.tell(PoisonPill.getInstance,self)
  
  def failedToCompile(stackTrace: String): Unit =
    println(s"$sender failed to compiled, stacktrace returned: \n $stackTrace")


}

object AnalysisTask {
  object Message {
    case object StartAnalysis
    case class Setup(analyzer: Analyser[Any], jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long])
    case class SetupNewAnalyser(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long])
    case class Ready(messages: Int)
    case class NextStep(analyzer: Analyser[Any], jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long])
    case class NextStepNewAnalyser(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long])
    case class EndStep(messages: Int, voteToHalt: Boolean)
    case class Finish(analyzer: Analyser[Any], jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long])
    case class FinishNewAnalyser(jobID: String, superStep: Int, timestamp: Long, analysisType: AnalysisType.Value, window: Long, windowSet: Array[Long])
    case class ReturnResults(results: Any)
    case class MessagesReceived(workerID: Int, receivedMessages: Int, sentMessages: Int)
    case class CheckMessages(jobID: ViewJob, superstep: Int)
    case object ReaderWorkersOnline
    case object ReaderWorkersAck
    case class AnalyserPresentCheck(className: String)
    case object AnalyserPresent
    case object ClassMissing
    case class FailedToCompile(stackTrace: String)
    case class CompileNewAnalyser(analyser: String, args: Array[String], name: String)
    case class TimeCheck(timestamp: Long)
    case class TimeResponse(ok: Boolean, time: Long)
  }
}
