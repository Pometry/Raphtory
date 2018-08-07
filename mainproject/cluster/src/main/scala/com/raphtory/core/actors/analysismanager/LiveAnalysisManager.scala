package com.raphtory.core.actors.analysismanager

import java.io.FileNotFoundException

import scala.concurrent.duration._
import akka.actor.Cancellable
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}

import scala.concurrent.ExecutionContext.Implicits.global
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils
import com.raphtory.core.analysis.Analyser
import com.raphtory.core.storage.controller.GraphRepoProxy
import scala.sys.process._
import scala.io.Source

abstract class LiveAnalysisManager extends RaphtoryActor {
  private var managerCount : Int = 0
  private var currentStep  = 0L
  private var networkSize  = 0
  private var networkSizeReplies = 0
  private var networkSizeTimeout : Cancellable = null
  private var readyCounter       = 0
  private var currentStepCounter = 0
  private var toSetup            = true
  protected def analyserName:String = generateAnalyzer.getClass.getName

  private val debug = false
  private var newAnalyser:Boolean = false

  protected val mediator     = DistributedPubSub(context.system).mediator
  protected var steps  : Long= 0L
  protected var results      = Vector.empty[Any]
  protected var oldResults      = Vector.empty[Any]

  implicit val proxy : GraphRepoProxy.type = null

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
    context.system.scheduler.scheduleOnce(Duration(20, SECONDS), self, "start")
    steps = defineMaxSteps()
    //context.system.scheduler.schedule(Duration(5, SECONDS), Duration(10, MINUTES), self, "start") // Refresh networkSize and restart analysis currently
  }

  protected final def getNetworkSize  : Int = networkSize
  protected final def getManagerCount : Int = managerCount
  protected final def sendGetNetworkSize() : Unit = {
    networkSize        = 0
    networkSizeReplies = 0
    networkSizeTimeout = context.system.scheduler.scheduleOnce(Duration(30, SECONDS), self, "networkSizeTimeout")
    mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, GetNetworkSize())
  }

  override def receive: Receive = {
    //case "analyse" => analyse()
    case "start" => {
      if(debug)println("Starting")
      checkClusterSize
      sendGetNetworkSize()
    }
    case "networkSizeTimeout" => {
      sendGetNetworkSize()
      if(debug)println("Timeout networkSize")
    }
    case Results(result) => {
      if(debug)println("Printing results")
      this.processResults(result)
    }
    case PartitionsCount(newValue) => {
      if(debug)println(s"received new count of $newValue")
      managerCount = newValue
    }
    case NetworkSize(size) => {
      if(debug)println("Received NetworkSize packet")
        networkSize += size
        networkSizeReplies += 1
        if (networkSizeReplies >= getManagerCount) {
          networkSizeTimeout.cancel()
          if(debug)println(steps)
          this.defineMaxSteps()
          if(debug)println(networkSize)
          mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, AnalyserPresentCheck(this.generateAnalyzer.getClass.getName.replace("$","")))
        }
      }

    case AnalyserPresent() => {
      mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, Setup(this.generateAnalyzer))
    }

    case ClassMissing() => {
      if(debug)println(s"$sender does not have analyser, sending now")
      var code = missingCode()
      newAnalyser = true
      sender() ! SetupNewAnalyser(code,analyserName)
    }

    case FailedToCompile(stackTrace) => {
      if(debug)println(s"${sender} failed to compiled, stacktrace returned: \n $stackTrace")
    }

    case Ready() => {
      if(debug)println("Received ready")
      readyCounter += 1
      if(debug)println(s"$readyCounter / ${getManagerCount}")
      if (readyCounter == getManagerCount) {
        readyCounter = 0
        currentStep = 1
        results = Vector.empty[Any]
        if(debug)println(s"Sending analyzer")
        if(newAnalyser)
          mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, NextStepNewAnalyser(analyserName))
        else
          mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, NextStep(this.generateAnalyzer))
      }
    }
    case EndStep(res) => {
      if(debug)println("EndStep")
      currentStepCounter += 1
      results +:= res
      if(debug)println(s"$currentStepCounter / $getManagerCount : $currentStep / $steps")
      if (currentStepCounter >= getManagerCount) {
        if (currentStep == steps || this.checkProcessEnd()) {
          // Process results
          this.processResults(results)
          currentStepCounter = 0
          context.system.scheduler.scheduleOnce(Duration(20, SECONDS), self, "start")
        } else {
          if(debug)println(s"Sending new step")
          oldResults = results
          results = Vector.empty[Any]
          currentStep += 1
          currentStepCounter = 0
          if(newAnalyser)
            mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, NextStepNewAnalyser(analyserName))
          else
            mediator ! DistributedPubSubMediator.Publish(Utils.readersTopic, NextStep(this.generateAnalyzer))
        }
      }
    }
    case _ => processOtherMessages(_)
  }

  def checkClusterSize ={
    if(debug)println("LAM SENDING REQUEST TO WATCHDOG")
      mediator ! DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionCount, false)
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

  //  private final def analyse() ={
  //      for(i <- 0 until managerCount){
  //        mediator ! DistributedPubSubMediator.Send(s"/user/Manager_$i",
  //          LiveAnalysis(
  //            Class.forName(sys.env.getOrElse("ANALYSISTASK", classOf[GabPageRank].getClass.getName))
  //              .newInstance().asInstanceOf[Analyser]),
  //          false)
  //      }
  //  }
}
