package com.raphtory.core.actors.AnalysisManager

import akka.actor.{Actor, ActorRef, InvalidActorNameException, PoisonPill, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.analysis.Tasks.AnalysisTask.Message.FailedToCompile
import com.raphtory.core.analysis.api.{Analyser, BlankAnalyser, LoadExternalAnalyser}
import com.raphtory.analysis.Tasks.LiveTasks.{BWindowedLiveAnalysisTask, LiveAnalysisTask, WindowedLiveAnalysisTask}
import com.raphtory.analysis.Tasks.RangeTasks.{BWindowedRangeAnalysisTask, RangeAnalysisTask, WindowedRangeAnalysisTask}
import com.raphtory.analysis.Tasks.ViewTasks.{BWindowedViewAnalysisTask, ViewAnalysisTask, WindowedViewAnalysisTask}
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.Message._
import com.raphtory.core.actors.AnalysisManager.AnalysisRestApi._
import com.raphtory.core.actors.ClusterManagement.WatchDog.Message._
import com.raphtory.core.actors.RaphtoryActor

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}
import scala.language.postfixOps
case class StartAnalysis()
class AnalysisManager() extends RaphtoryActor{
  implicit val executionContext = context.system.dispatchers.lookup("misc-dispatcher")
  implicit val timeout: Timeout = 10.seconds
  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  val debug = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean
  val currentTasks = ParTrieMap[String, ActorRef]()
  private var safe = false
  protected var managerCount: Int = 0 //Number of Managers in the Raphtory Cluster

  override def preStart() {
    context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "startUp")
  }

  override def receive: Receive = {
    case "startUp"     => clusterReadyForAnalysis() //first ask the watchdog if it is safe to do analysis and what the size of the cluster is //when the watchdog responds, set the new value and message each Reader Worker
    case PartitionsCount(newValue)   => managerCount = newValue     //for if managerCount is republished
    case request:LiveAnalysisRequest =>  if(!safe) notYet(request) else spawnLiveAnalysisManager(request)
    case request:ViewAnalysisRequest =>  if(!safe) notYet(request) else spawnViewAnalysisManager(request)
    case request:RangeAnalysisRequest => if(!safe) notYet(request) else spawnRangeAnalysisManager(request)
    case RequestResults(jobID) => checkResults(jobID)
    case KillTask(jobID) => killJob(jobID)
  }

  def checkResults(jobID: String) = {
    if(currentTasks contains jobID){
      try {
        val future = currentTasks(jobID) ? RequestResults(jobID)
        Await.result(future, timeout.duration) match {
          case results:ResultsForApiPI => sender ! results
        }
      } catch {
        case _: java.util.concurrent.TimeoutException =>
      }
    }
    else sender() ! JobDoesntExist
  }

  def killJob(jobID: String) = {
    if(currentTasks contains jobID){
      currentTasks(jobID) ! PoisonPill
      currentTasks remove(jobID)
      sender() ! JobKilled
    }
    else sender()! JobDoesntExist
  }

  def spawnLiveAnalysisManager(request: LiveAnalysisRequest): Unit = {
    try {
      val jobID = request.analyserName+"_"+System.currentTimeMillis().toString
      println(s"Live Analysis Task received, your job ID is ${jobID}")
      val args = request.args
      val repeatTime = request.repeatTime
      val eventTime = request.eventTime
      val analyserFile = request.rawFile
      val buildAnalyser = getAnalyser(request.analyserName,args,request.rawFile)
      val newAnalyser = buildAnalyser._1
      val analyser = buildAnalyser._2
      if(analyser.isInstanceOf[BlankAnalyser])
        return
      val ref= request.windowType match {
        case "false" =>
          context.system.actorOf(Props(new LiveAnalysisTask(managerCount, jobID,args, analyser,repeatTime,eventTime,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"), s"LiveAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(Props(new WindowedLiveAnalysisTask(managerCount, jobID,args, analyser,repeatTime,eventTime, request.windowSize,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"), s"LiveAnalysisTask__windowed_$jobID")
        case "batched" =>
          context.system.actorOf(Props(new BWindowedLiveAnalysisTask(managerCount, jobID,args, analyser,repeatTime,eventTime, request.windowSet,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"), s"LiveAnalysisTask__batchWindowed_$jobID")
      }
      currentTasks put (jobID,ref)
  }
    catch {
      case e:InvalidActorNameException => println("Name non unique please kill other job first")
    }
  }
  def spawnViewAnalysisManager(request: ViewAnalysisRequest): Unit = {

    try{
      val jobID = request.analyserName+"_"+System.currentTimeMillis().toString
      println(s"View Analysis Task received, your job ID is ${jobID}")
      val timestamp = request.timestamp
      val args = request.args
      val buildAnalyser = getAnalyser(request.analyserName,args,request.rawFile)
      val newAnalyser = buildAnalyser._1
      val analyser = buildAnalyser._2
      if(analyser.isInstanceOf[BlankAnalyser])
        return

      val analyserFile = request.rawFile
      val ref =request.windowType match {
        case "false" =>
          context.system.actorOf(Props(new ViewAnalysisTask(managerCount,jobID,args,analyser, timestamp,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"), s"ViewAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(
            Props(new WindowedViewAnalysisTask(managerCount,jobID,args, analyser, timestamp, request.windowSize,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"),
            s"ViewAnalysisTask_windowed_$jobID"
          )
        case "batched" =>
          context.system.actorOf(
            Props(new BWindowedViewAnalysisTask(managerCount,jobID, args,analyser, timestamp, request.windowSet,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"),
            s"ViewAnalysisTask_batchWindowed_$jobID"
          )
      }
      currentTasks put (jobID,ref)
    }
    catch {
      case e:InvalidActorNameException => println("Name non unique please kill other job first")
    }
  }

  def spawnRangeAnalysisManager(request: RangeAnalysisRequest): Unit = {
    try{
      val jobID = request.analyserName+"_"+System.currentTimeMillis().toString
      println(s"Range Analysis Task received, your job ID is ${jobID}, running ${request.analyserName}, between ${request.start} and ${request.end} jumping ${request.jump} at a time.")
      val start = request.start
      val end   = request.end
      val jump  = request.jump
      val args = request.args
      val analyserFile = request.rawFile
      val buildAnalyser = getAnalyser(request.analyserName,args,request.rawFile)
      val newAnalyser = buildAnalyser._1
      val analyser = buildAnalyser._2
      if(analyser.isInstanceOf[BlankAnalyser])
        return
      val ref = request.windowType match {
        case "false" =>
          context.system
            .actorOf(Props(new RangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"), s"RangeAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(
            Props(new WindowedRangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump, request.windowSize,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"),
            s"RangeAnalysisTask_windowed_$jobID"
          )
        case "batched" =>
          context.system.actorOf(
            Props(new BWindowedRangeAnalysisTask(managerCount,jobID,args, analyser, start, end, jump, request.windowSet,newAnalyser,analyserFile)).withDispatcher("analysis-dispatcher"),
            s"RangeAnalysisTask_batchWindowed_$jobID"
          )
      }
      currentTasks put (jobID,ref)
    }
    catch {
      case e:InvalidActorNameException => println("Name non unique please kill other job first")
    }
  }

  private def clusterReadyForAnalysis(): Unit =
    if (!safe)
      try {
        implicit val timeout: Timeout = Timeout(10 seconds) //time to wait for watchdog response
        val future                    = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest, false) //ask if the cluster is safe to use
        if(Await.result(future, timeout.duration).asInstanceOf[ClusterStatusResponse].clusterUp) { //if it is
          val future                  = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionCount, false) //ask how many partitions there are
          managerCount = Await.result(future, timeout.duration).asInstanceOf[PartitionsCount].count //when they respond set the partition manager count to this value
          safe = true
          println("Cluster ready for Analysis")
        }
        else{
          context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "startUp")
        }
      } catch {
        case e: java.util.concurrent.TimeoutException => context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, "startUp")
      }

  private def notYet(request:AnalysisRequest) = {
    //println("Cluster not ready for analysis yet, resubmitting in 5 seconds")
    context.system.scheduler.scheduleOnce(Duration(5, SECONDS), self, request)
  }

  private def getAnalyser(analyserName:String,args:Array[String],rawFile:String): (Boolean,Analyser[Any]) ={
    try {
      (false,Class.forName(analyserName).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser[Any]])
    } catch {
      case e:NoSuchMethodException =>
        try {
          (false, Class.forName(analyserName).getConstructor().newInstance().asInstanceOf[Analyser[Any]])
        }
        catch {
          case e:ClassNotFoundException => processCompileNewAnalyserRequest(rawFile,args)
        }
    }
  }

  def processCompileNewAnalyserRequest(rawFile:String,args:Array[String]): (Boolean,Analyser[Any]) = {
    var analyser: Analyser[Any] = new BlankAnalyser(args)
    try{
      analyser = LoadExternalAnalyser(rawFile,args).newAnalyser
    }
    catch {
      case e:Exception => {
        sender ! FailedToCompile(e.getStackTrace.toString)
        println(e.getMessage)
        println(analyser.getClass)
      }
    }
    (true,analyser)
  }

}

object AnalysisManager {
  object Message {
    case class RequestResults(jobID:String)
    case class KillTask(jobID:String)
    case object JobKilled
    case class ResultsForApiPI(results:Array[String])
    case object JobDoesntExist
  }
}