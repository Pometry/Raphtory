package com.raphtory.core.analysis.Managers

import java.io.FileNotFoundException
import java.util.Date

import scala.concurrent.duration._
import akka.actor.{Actor, Cancellable}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

import scala.concurrent.ExecutionContext.Implicits.global
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils
import com.raphtory.core.analysis.API.Analyser

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParTrieMap
import scala.sys.process._
import scala.io.Source

abstract class AnalysisManager(jobID:String, analyser: Analyser) extends Actor {
  protected var managerCount : Int = 0 //Number of Managers in the Raphtory Cluster
  private var currentSuperStep  = 0 //SuperStep the algorithm is currently on

  //Communication Counters
  private var ReaderACKS = 0 //Acks from the readers to say they are online
  private var ReaderAnalyserACKS = 0 //Ack to show that the chosen analyser is present within the partition
  private var TimeOKFlag = true //flag to specify if the job is ok to run if temporal
  private var TimeOKACKS = 0 //counter to see that all partitions have responded
  private var workersFinishedSetup = 0 //Acks from workers to see how many have finished the setup stage of analysis
  private var workersFinishedSuperStep = 0 //Acks from workers to say they have finished the superstep
  private var messageLogACKS = 0 //Acks from Workers with the amount of messages they have sent and received

  private var totalReceivedMessages = 0 //Total number of messages received by the workers
  private var totalSentMessages = 0 //Total number of messages sent by the workers

  private var voteToHalt = true // If the workers have decided to halt

  private var newAnalyser:Boolean = false //if the analyser is not present in the readers

  private var networkSizeTimeout : Cancellable = null //for restarting the analysers if it joins too quickly
  private val debug = false //for printing debug messages

  protected val mediator     = DistributedPubSub(context.system).mediator

  private var results:ArrayBuffer[Any]      = mutable.ArrayBuffer[Any]()
  private var oldResults:ArrayBuffer[Any]   = mutable.ArrayBuffer[Any]()

  def result() = results
  def oldResult() = oldResults

  protected def analysisType():AnalysisType.Value


  protected var steps  : Long= 0L // number of supersteps before returning
  def timestamp(): Long = -1L //for view
  def windowSize(): Long = -1L //for windowing
  def windowSet(): Array[Long] = Array.empty //for sets of windows


  private def processOtherMessages(value : Any) : Unit = {println ("Not handled message" + value.toString)}

  protected def generateAnalyzer : Analyser = analyser
  protected def analyserName:String = generateAnalyzer.getClass.getName
  protected final def getManagerCount : Int = managerCount
  protected final def getWorkerCount : Int = managerCount*10
  protected def processResults() = analyser.processResults(results,oldResults)

  private def resetCounters() = {
    ReaderACKS = 0 //Acks from the readers to say they are online
    ReaderAnalyserACKS = 0 //Ack to show that the chosen analyser is present within the partition
    workersFinishedSetup = 0 //Acks from workers to see how many have finished the setup stage of analysis
    workersFinishedSuperStep = 0 //Acks from workers to say they have finished the superstep
    messageLogACKS = 0 //Acks from Workers with the amount of messages they have sent and received
    totalReceivedMessages = 0 //Total number of messages received by the workers
    totalSentMessages = 0 //Total number of messages sent by the workers
    currentSuperStep = 0
  }


  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.liveAnalysisTopic, self)

  override def preStart(): Unit = {
    context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, "start")
    steps = analyser.defineMaxSteps()
  }

  override def receive: Receive = {
    case "start" => checkClusterSize //first ask the watchdog what the size of the cluster is
    case PartitionsCountResponse(newValue)=> watchdogResponse(newValue) //when the watchdog responds, set the new value and message each Reader Worker
    case ReaderWorkersACK() => readerACK() //count up number of acks and if == number of workers, check if analyser present
    case AnalyserPresent() => analyserPresent() //analyser confirmed to be present within workers, send setup request to workers
    case TimeResponse(ok) => timeResponse(ok)   //checking if the timestamps are ok within all partitions
    case "recheckTime" => timeRecheck()  //if the time was previous out of scope, wait and then recheck
    case Ready(messagesSent) => ready(messagesSent) //worker has completed setup and is ready to roll -- send nextstep
    case EndStep(result,messages,voteToHalt) => endStep(result,messages,voteToHalt) //worker has finished the step
    case "restart" => restart()

    case MessagesReceived(workerID,real,receivedMessages,sentMessages) => messagesReceieved(workerID,real,receivedMessages,sentMessages)

    case "networkSizeTimeout" => networkSizeFail() //restart contact with readers
    case ClassMissing() => classMissing() //If the class is missing, send the raw source file
    case FailedToCompile(stackTrace) => failedToCompile(stackTrace) //Your code is broke scrub
    case PartitionsCount(newValue) => managerCount = newValue //for when managerCount is republished
    case _ => processOtherMessages(_) //incase some random stuff comes through
  }

  protected def checkClusterSize: Unit = mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionCount, false)

  def watchdogResponse(newValue: Int)= {
    managerCount = newValue
    ReaderACKS = 0
    networkSizeTimeout = context.system.scheduler.scheduleOnce(Duration(30, SECONDS), self, "networkSizeTimeout")
    for(worker <- Utils.getAllReaders(managerCount)) {
      mediator ! DistributedPubSubMediator.Send(worker, ReaderWorkersOnline(), false)
    }
  }

  def readerACK() ={
    if(debug)println("Received NetworkSize packet")
    ReaderACKS += 1
    if (ReaderACKS == getManagerCount) {
      networkSizeTimeout.cancel()
      for(worker <- Utils.getAllReaders(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")),false)
    }
  }

  def analyserPresent() = {
    ReaderAnalyserACKS += 1
    if (ReaderAnalyserACKS == getManagerCount) {
      networkSizeTimeout.cancel()
      for(worker <- Utils.getAllReaders(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp),false)
    }
  }

  def timeResponse(ok:Boolean) = {
    if(!ok)
      TimeOKFlag =false
    TimeOKACKS +=1
    if(TimeOKACKS==getManagerCount) {
      if (TimeOKFlag)
        for (worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, Setup(this.generateAnalyzer, jobID, currentSuperStep,timestamp,analysisType(),windowSize(),windowSet()), false)
      else {
        println(s"${new Date(timestamp())} is yet to be ingested. Backing off to 10 seconds and retrying")
        context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, "recheckTime")
      }
      TimeOKACKS = 0
      TimeOKFlag =true
    }
  }

  def timeRecheck() = {
    for(worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, TimeCheck(timestamp),false)
  }

  def ready(messages:Int) = {
    if(debug)println("Received ready")
    workersFinishedSetup += 1
    totalSentMessages += messages
    if(debug)println(s"$workersFinishedSetup / ${getWorkerCount}")
    if (workersFinishedSetup == getWorkerCount) {
      workersFinishedSetup = 0
      syncMessages()
    }
  }

  def endStep(result:Any,messages:Int,voteToHalt:Boolean) = {
    workersFinishedSuperStep += 1
    results += result
    totalSentMessages += messages
    if (!voteToHalt) this.voteToHalt = false
    if(debug)println(s"$workersFinishedSuperStep / $getWorkerCount : $currentSuperStep / $steps")
    if (workersFinishedSuperStep == getWorkerCount) {
      if (currentSuperStep == steps || analyser.checkProcessEnd(results,oldResults)) {
        processResults()
        results = mutable.ArrayBuffer[Any]()
        oldResults = mutable.ArrayBuffer[Any]()
       // for(worker <- Utils.getAllReaderWorkers(managerCount))
       //   mediator ! DistributedPubSubMediator.Send(worker, Finish(this.generateAnalyzer,jobID,currentSuperStep),false)
        resetCounters()
        context.system.scheduler.scheduleOnce(Duration(10, MILLISECONDS), self, "restart")
      }
      else {
        this.voteToHalt = true
        workersFinishedSuperStep = 0
        oldResults = results
        results = mutable.ArrayBuffer[Any]()
        syncMessages()
      }
    }
  }


  private def syncMessages() = {
    if(totalSentMessages == 0){
      println("inside")
      currentSuperStep += 1
      if(newAnalyser)
        for(worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, NextStepNewAnalyser(analyserName,jobID,currentSuperStep,timestamp,analysisType:AnalysisType.Value,windowSize(),windowSet()),false)
      else {
        for(worker <- Utils.getAllReaderWorkers(managerCount)){
          mediator ! DistributedPubSubMediator.Send(worker, NextStep(this.generateAnalyzer, jobID, currentSuperStep,timestamp,analysisType:AnalysisType.Value,windowSize(),windowSet()),false)
        }
      }
    }
    else {
      totalSentMessages = 0
      totalReceivedMessages = 0
      for(worker <- Utils.getAllReaderWorkers(managerCount))
        mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(currentSuperStep),false)
    }
  }

  def messagesReceieved(workerID:Int,real:Int,receivedMessages: Int,sentMessages:Int) = {
    messageLogACKS +=1
    totalReceivedMessages += receivedMessages
    totalSentMessages += sentMessages
    if(real != receivedMessages)
      println(s"superstep $currentSuperStep workerID: $workerID -- messages Received $receivedMessages/$real  -- total $totalReceivedMessages-- sent messages $sentMessages/$totalSentMessages")
    if(messageLogACKS == getWorkerCount) {
      messageLogACKS =0
      if(totalReceivedMessages == totalSentMessages){
        currentSuperStep+=1
        totalSentMessages = 0
        totalReceivedMessages = 0
        for(worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, NextStep(this.generateAnalyzer,jobID,currentSuperStep,timestamp,analysisType:AnalysisType.Value,windowSize(),windowSet()),false)
      }
      else {
        println(s"checking, $totalReceivedMessages/$totalSentMessages")
        totalReceivedMessages =0
        totalSentMessages = 0
        Thread.sleep(1000)
        for(worker <- Utils.getAllReaderWorkers(managerCount))
          mediator ! DistributedPubSubMediator.Send(worker, CheckMessages(currentSuperStep),false)
          //mediator ! DistributedPubSubMediator.Send(worker, NextStep(this.generateAnalyzer,jobID,currentSuperStep,timestamp,windowSize(),windowSet()),false)

       //`
      }
    }
  }

  def restart() =
    for(worker <- Utils.getAllReaders(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")),false)

  def partitionsHalting() = voteToHalt


  /////HERE BE DRAGONS, GO NO FURTHER
  def networkSizeFail() {
    if(debug)println("Timeout networkSize")
    ReaderACKS = 0
    networkSizeTimeout = context.system.scheduler.scheduleOnce(Duration(30, SECONDS), self, "networkSizeTimeout")
    for(worker <- Utils.getAllReaderWorkers(managerCount))
      mediator ! DistributedPubSubMediator.Send(worker, ReaderWorkersOnline(),false)
  }

  def classMissing() {
    if(debug)println(s"$sender does not have analyser, sending now")
    var code = missingCode()
    newAnalyser = true
    sender() ! CompileNewAnalyser(code,analyserName)
  }
  def failedToCompile(stackTrace:String): Unit = {
    if(debug)println(s"${sender} failed to compiled, stacktrace returned: \n $stackTrace")

  }

  def missingCode() : String = {
    val file = generateAnalyzer.getClass.getName.replaceAll("\\.","/").replaceAll("$","")
    var code = ""
    if(debug)println("pwd" !)
    try code = readClass(s"../$file.scala") //if inside a contained and the src has been copied
    catch { case e:FileNotFoundException=> code = readClass(s"cluster/src/main/scala/$file.scala")} //if we are running locally inside of the the mainproject folder
    code
  }

  def readClass(file:String):String = {
    val bufferedSource = Source.fromFile(file)
    var code =""
    for (line <- bufferedSource.getLines) {
      if(line.startsWith("class ") && line.contains("extends Analyser")) //name of class must be replaced with "new Analyser"
        if(line.contains("{"))
          code += "new Analyser{ \n"
        else
          code += "new Analyser \n"
      else if(!line.startsWith("package com.")) //have to also remove package line
        code += s"$line\n"
    }
    bufferedSource.close
    code
  }
}
