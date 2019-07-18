package com.raphtory.core.components.AnalysisManager

import java.io.FileNotFoundException

import scala.concurrent.duration._
import akka.actor.{Actor, Cancellable}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

import scala.concurrent.ExecutionContext.Implicits.global
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils
import com.raphtory.core.analysis.{Analyser, GraphRepoProxy}

import scala.sys.process._
import scala.io.Source

abstract class LiveAnalysisManager(jobID:String) extends Actor {
  private var managerCount : Int = 0
  private var currentStep  = 0
  private var ReaderACKS = 0
  private var ReaderAnalysersPresent = 0
  private var networkSizeTimeout : Cancellable = null
  private var readyCounter       = 0
  private var currentStepCounter = 0
  private var toSetup            = true
  private var messageCounter = 0
  private var totalReceivedMessages = 0
  private var totalSentMessages = 0
  protected def analyserName:String = generateAnalyzer.getClass.getName

  private val debug = true
  private var newAnalyser:Boolean = false

  protected val mediator     = DistributedPubSub(context.system).mediator
  protected var steps  : Long= 0L
  protected var results      = Vector.empty[Any]
  protected var oldResults      = Vector.empty[Any]

 /******************** STUFF TO DEFINE *********************/
  protected def defineMaxSteps() : Int
  protected def generateAnalyzer : Analyser
  protected def processResults(result : Any) : Unit
  protected def processOtherMessages(value : Any) : Unit
  protected def checkProcessEnd() : Boolean = false
  /******************** STUFF TO DEFINE *********************/

  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.liveAnalysisTopic, self)

  override def preStart(): Unit = {

    context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, "start")
    steps = defineMaxSteps()
    //context.system.scheduler.schedule(Duration(5, SECONDS), Duration(10, MINUTES), self, "start") // Refresh networkSize and restart analysis currently
  }

  protected final def getManagerCount : Int = managerCount
  protected final def getWorkerCount : Int = managerCount*10

  override def receive: Receive = {
    case "start" => checkClusterSize //first ask the watchdog what the size of the cluster is
    case PartitionsCountResponse(newValue)=> watchdogResponse(newValue) //when the watchdog responds, set the new value and message each Reader Worker
    case ReaderWorkersACK() => readerACK() //count up number of acks and if == number of workers, check if analyser present
    case AnalyserPresent() => analyserPresent() //analyser confirmed to be present within workers, send setup request to workers
    case Ready(messagesSent) => ready(messagesSent) //worker has completed setup and is ready to roll -- send nextstep
    case EndStep(result,messages) => endStep(result,messages) //worker has finished the step
    case "restart" => restart()

    case MessagesReceived(receivedMessages,sentMessages) => messagesReceieved(receivedMessages,sentMessages)

    case "networkSizeTimeout" => networkSizeFail() //restart contact with readers
    case ClassMissing() => classMissing() //If the class is missing, send the raw source file
    case FailedToCompile(stackTrace) => failedToCompile(stackTrace) //Your code is broke scrub
    case PartitionsCount(newValue) => managerCount = newValue //for when managerCount is republished
    case _ => processOtherMessages(_) //incase some random stuff comes through
  }

  def checkClusterSize =
    mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionCount, false)

  def watchdogResponse(newValue: Int)= {
    managerCount = newValue
    ReaderACKS = 0
    networkSizeTimeout = context.system.scheduler.scheduleOnce(Duration(30, SECONDS), self, "networkSizeTimeout")
    mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, ReaderWorkersOnline())
  }

  def readerACK() ={
    if(debug)println("Received NetworkSize packet")
    ReaderACKS += 1
    if (ReaderACKS == getManagerCount) {
      networkSizeTimeout.cancel()
      mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")))
    }
  }

  def analyserPresent() = {
    ReaderAnalysersPresent += 1
    if (ReaderACKS == getManagerCount) {
      networkSizeTimeout.cancel()
      mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, Setup(this.generateAnalyzer,jobID,currentStep))
    }
  }

  def ready(messages:Int) = {
    if(debug)println("Received ready")
    readyCounter += 1
    totalSentMessages += messages
    if(debug)println(s"$readyCounter / ${getWorkerCount}")
    if (readyCounter == getWorkerCount) {
      readyCounter = 0
      currentStep = 1
      results = Vector.empty[Any]
      if(totalSentMessages == 0){
        if(newAnalyser)
          mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, NextStepNewAnalyser(analyserName,jobID,currentStep))
        else
          mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, NextStep(this.generateAnalyzer,jobID,currentStep))
      }
      else{
        totalSentMessages = 0
        totalReceivedMessages = 0
        if(debug)println("Sending check Messages")
        mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, CheckMessages())

      }
    }
  }

  def endStep(result:Any,messages:Int) = {
    currentStepCounter += 1
    results +:= result
    totalSentMessages += messages
    if(debug)println(s"$currentStepCounter / $getWorkerCount : $currentStep / $steps")
    if (currentStepCounter == getWorkerCount) {
      if (currentStep == steps || this.checkProcessEnd()) {
        // Process results
        this.processResults(results)
        currentStepCounter = 0
        currentStep = 0
        context.system.scheduler.scheduleOnce(Duration(2, SECONDS), self, "restart")
        totalSentMessages = 0
        totalReceivedMessages =0
      }
      else {
        if(debug)println(s"Sending new step")
        oldResults = results
        results = Vector.empty[Any]
        currentStep += 1
        currentStepCounter = 0
        if(totalSentMessages == 0){
          if(newAnalyser)
            mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, NextStepNewAnalyser(analyserName,jobID,currentStep))
          else
            mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, NextStep(this.generateAnalyzer,jobID,currentStep))
        }
        else {
          totalSentMessages = 0
          totalReceivedMessages = 0
          mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, CheckMessages())
        }
      }
    }
  }

  def messagesReceieved(receivedMessages: Int,sentMessages:Int) = {
    messageCounter +=1
    totalReceivedMessages += receivedMessages
    totalSentMessages += sentMessages
    //println("messages Received "+totalReceivedMessages)
    if(messageCounter == getWorkerCount) {
      messageCounter =0

      if(totalReceivedMessages == totalSentMessages){
        totalSentMessages = 0
        totalReceivedMessages = 0
        mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, NextStep(this.generateAnalyzer,jobID,currentStep))
      }
      else {
        println(s"checking, $totalReceivedMessages/$totalSentMessages")
        totalReceivedMessages =0
        totalSentMessages = 0
        Thread.sleep(100)
        mediator ! DistributedPubSubMediator.Publish(Utils.readersWorkerTopic, CheckMessages())
      }
    }
  }

  def restart() =
    mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")))


  /////HERE BE DRAGONS, GO NO FURTHER
  def networkSizeFail() {
    if(debug)println("Timeout networkSize")
    ReaderACKS = 0
    networkSizeTimeout = context.system.scheduler.scheduleOnce(Duration(30, SECONDS), self, "networkSizeTimeout")
    mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, ReaderWorkersOnline())
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
