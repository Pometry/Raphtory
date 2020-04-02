package com.raphtory.core.analysis.Tasks

import java.io.FileNotFoundException
import java.util.Date

import akka.actor.{Actor, Cancellable, PoisonPill}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.StartAnalysis
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source
import scala.sys.process._

abstract class AnalysisTask(jobID: String, analyser: Analyser, managerCount:Int) extends Actor {
  private var currentSuperStep    = 0 //SuperStep the algorithm is currently on

  private var local: Boolean = Utils.local

  //Communication Counters
  private var ReaderACKS               = 0    //Acks from the readers to say they are online
  private var ReaderAnalyserACKS       = 0    //Ack to show that the chosen analyser is present within the partition
  private var TimeOKFlag               = true //flag to specify if the job is ok to run if temporal
  private var TimeOKACKS               = 0    //counter to see that all partitions have responded
  private var workersFinishedSetup     = 0    //Acks from workers to see how many have finished the setup stage of analysis
  private var workersFinishedSuperStep = 0    //Acks from workers to say they have finished the superstep
  private var workerResultsReceived    = 0    //Acks from workers when sending results back to analysis manager
  private var messageLogACKS           = 0    //Acks from Workers with the amount of messages they have sent and received

  private var totalReceivedMessages = 0 //Total number of messages received by the workers
  private var totalSentMessages     = 0 //Total number of messages sent by the workers

  private var voteToHalt = true // If the workers have decided to halt

  private var newAnalyser: Boolean = false //if the analyser is not present in the readers

  private val debug                = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean //for printing debug messages

  protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  private var results: ArrayBuffer[Any] = mutable.ArrayBuffer[Any]()

  def result() = results

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
  def timestamp(): Long        = -1L         //for view
  def windowSize(): Long       = -1L         //for windowing
  def windowSet(): Array[Long] = Array.empty //for sets of windows

  private def processOtherMessages(value: Any): Unit = println("Not handled message" + value.toString)
  protected def generateAnalyzer: Analyser =
    if (local) Class.forName(analyser.getClass.getCanonicalName).newInstance().asInstanceOf[Analyser] else analyser
  protected def analyserName: String            = generateAnalyzer.getClass.getName
  final protected def getManagerCount: Int      = managerCount
  final protected def getWorkerCount: Int       = managerCount * 10
  protected def processResults(timeStamp: Long) = analyser.processResults(results, timeStamp,viewCompleteTime())

  private def resetCounters() = {
    ReaderACKS = 0               //Acks from the readers to say they are online
    ReaderAnalyserACKS = 0       //Ack to show that the chosen analyser is present within the partition
    workersFinishedSetup = 0     //Acks from workers to see how many have finished the setup stage of analysis
    workersFinishedSuperStep = 0 //Acks from workers to say they have finished the superstep
    messageLogACKS = 0           //Acks from Workers with the amount of messages they have sent and received
    totalReceivedMessages = 0    //Total number of messages received by the workers
    totalSentMessages = 0        //Total number of messages sent by the workers
    workerResultsReceived = 0    //Total number of acks from worker when they are sending results back
    currentSuperStep = 0
    results = mutable.ArrayBuffer[Any]()
  }

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.liveAnalysisTopic, self)

  override def preStart() {
    context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis())
  }

  override def receive: Receive = {
    case StartAnalysis()               => startAnalysis() //received message to start from Orchestrator
    case ReaderWorkersACK()            => readerACK() //count up number of acks and if == number of workers, check if analyser present
    case AnalyserPresent()             => analyserPresent() //analyser confirmed to be present within workers, send setup request to workers
    case TimeResponse(ok, time)        => timeResponse(ok, time) //checking if the timestamps are ok within all partitions
    case "recheckTime"                 => timeRecheck() //if the time was previous out of scope, wait and then recheck
    case Ready(messagesSent)           => ready(messagesSent) //worker has completed setup and is ready to roll -- send nextstep
    case EndStep(messages, voteToHalt) => endStep(messages, voteToHalt) //worker has finished the step
    case ReturnResults(results)        => finaliseJob(results) //worker has finished teh job and is returned the results
    case "restart"                     => restart()
    case MessagesReceived(workerID, real, receivedMessages, sentMessages) =>
      messagesReceieved(workerID, real, receivedMessages, sentMessages)

    case ClassMissing()              => classMissing()              //If the class is missing, send the raw source file
    case FailedToCompile(stackTrace) => failedToCompile(stackTrace) //Your code is broke scrub
    case _                           => processOtherMessages(_)     //incase some random stuff comes through
  }

  def startAnalysis() = {
    for (worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, ReaderWorkersOnline(), false)
  }

  def readerACK() = {
    if (debug) println("Received NetworkSize packet")
    ReaderACKS += 1
    if (ReaderACKS == getManagerCount) {
      for (worker <- Utils.getAllReaders(managerCount))
        mediator ! DistributedPubSubMediator
          .Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$", "")), false)
    }
  }

  def analyserPresent() = {
    ReaderAnalyserACKS += 1
    if (ReaderAnalyserACKS == getManagerCount) {
      for (worker <- Utils.getAllReaders(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)
    }
  }

  def timeResponse(ok: Boolean, time: Long) = {
    if (!ok)
      TimeOKFlag = false
    TimeOKACKS += 1
    if (TimeOKACKS == getManagerCount) {
      stepCompleteTime() //reset step counter
      if (TimeOKFlag)
        if (analyser.defineMaxSteps() > 1)
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(
                    worker,
                    Setup(
                            this.generateAnalyzer,
                            jobID,
                            currentSuperStep,
                            timestamp,
                            analysisType(),
                            windowSize(),
                            windowSet()
                    ),
                    false
            )
        else
          for (worker <- Utils.getAllReaderWorkers(managerCount))
            mediator ! DistributedPubSubMediator.Send(
                    worker,
                    Finish(
                            this.generateAnalyzer,
                            jobID,
                            currentSuperStep,
                            timestamp,
                            analysisType(),
                            windowSize(),
                            windowSet()
                    ),
                    false
            )
      else {
        println(
                s"${timestamp()} is yet to be ingested, currently at ${time}. Retrying analysis in 10 seconds and retrying"
        )
        context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, "recheckTime")
      }
      TimeOKACKS = 0
      TimeOKFlag = true
    }
  }

  def timeRecheck() =
    for (worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp), false)

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
        for (worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(
                  worker,
                  Finish(
                          this.generateAnalyzer,
                          jobID,
                          currentSuperStep,
                          timestamp,
                          analysisType: AnalysisType.Value,
                          windowSize(),
                          windowSet()
                  ),
                  false
          )
      else {
        this.voteToHalt = true
        workersFinishedSuperStep = 0
        syncMessages()
      }
  }

  def finaliseJob(result: Any) = {
    results += result
    workerResultsReceived += 1
    if (workerResultsReceived == getWorkerCount) {
      processResults(timestamp())
      resetCounters()
      context.system.scheduler.scheduleOnce(Duration(10, MILLISECONDS), self, "restart")
    }
  }

  private def syncMessages() =
    if (totalSentMessages == 0) {
      currentSuperStep += 1
      if (newAnalyser)
        for (worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(
                  worker,
                  NextStepNewAnalyser(
                          analyserName,
                          jobID,
                          currentSuperStep,
                          timestamp,
                          analysisType: AnalysisType.Value,
                          windowSize(),
                          windowSet()
                  ),
                  false
          )
      else
        for (worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(
                  worker,
                  NextStep(
                          this.generateAnalyzer,
                          jobID,
                          currentSuperStep,
                          timestamp,
                          analysisType: AnalysisType.Value,
                          windowSize(),
                          windowSet()
                  ),
                  false
          )
    } else {
      totalSentMessages = 0
      totalReceivedMessages = 0
      for (worker <- Utils.getAllReaderWorkers(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(currentSuperStep), false)
    }

  def messagesReceieved(workerID: Int, real: Int, receivedMessages: Int, sentMessages: Int) = {
    messageLogACKS += 1
    totalReceivedMessages += receivedMessages
    totalSentMessages += sentMessages
    //if(real != receivedMessages)
    // println(s"superstep $currentSuperStep workerID: $workerID -- messages Received $receivedMessages/$real  -- total $totalReceivedMessages-- sent messages $sentMessages/$totalSentMessages")
    if (messageLogACKS == getWorkerCount) {
//      if (totalReceivedMessages != totalSentMessages)
//        println(
//                s"superstep $currentSuperStep workerID: $workerID -- $receivedMessages/$sentMessages $totalReceivedMessages/$totalSentMessages"
//        )
      messageLogACKS = 0
      if (totalReceivedMessages == totalSentMessages) {
        currentSuperStep += 1
        totalSentMessages = 0
        totalReceivedMessages = 0
        for (worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(
                  worker,
                  NextStep(
                          this.generateAnalyzer,
                          jobID,
                          currentSuperStep,
                          timestamp,
                          analysisType: AnalysisType.Value,
                          windowSize(),
                          windowSet()
                  ),
                  false
          )
      } else {
        //println(s"checking, $totalReceivedMessages/$totalSentMessages")
        totalReceivedMessages = 0
        totalSentMessages = 0
        currentSuperStep += 1
        for (worker <- Utils.getAllReaderWorkers(managerCount))
          //mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(currentSuperStep),false) //todo fix
          mediator ! DistributedPubSubMediator.Send(
                  worker,
                  NextStep(
                          this.generateAnalyzer,
                          jobID,
                          currentSuperStep,
                          timestamp,
                          analysisType: AnalysisType.Value,
                          windowSize(),
                          windowSet()
                  ),
                  false
          )
      }
    }
  }

  def restart() =
    for (worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator
        .Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$", "")), false)

  def partitionsHalting() = voteToHalt

  def killme() = self.tell(PoisonPill.getInstance,self)

  /////HERE BE DRAGONS, GO NO FURTHER

  def classMissing() {
    if (debug) println(s"$sender does not have analyser, sending now")
    var code = missingCode()
    newAnalyser = true
    sender() ! CompileNewAnalyser(code, analyserName)
  }
  def failedToCompile(stackTrace: String): Unit =
    if (debug) println(s"$sender failed to compiled, stacktrace returned: \n $stackTrace")

  def missingCode(): String = {
    val file = generateAnalyzer.getClass.getName.replaceAll("\\.", "/").replaceAll("$", "")
    var code = ""
    if (debug) println("pwd" !)
    try code = readClass(s"../$file.scala") //if inside a contained and the src has been copied
    catch { case e: FileNotFoundException => code = readClass(s"cluster/src/main/scala/$file.scala") } //if we are running locally inside of the the mainproject folder
    code
  }

  def readClass(file: String): String = {
    val bufferedSource = Source.fromFile(file)
    var code           = ""
    for (line <- bufferedSource.getLines)
      if (line.startsWith("class ") && line
            .contains("extends Analyser")) //name of class must be replaced with "new Analyser"
        if (line.contains("{"))
          code += "new Analyser{ \n"
        else
          code += "new Analyser \n"
      else if (!line.startsWith("package com.")) //have to also remove package line
        code += s"$line\n"
    bufferedSource.close
    code
  }
}
