package com.raphtory.core.analysis

import com.raphtory.core.model.communication.{ClusterStatusRequest, ClusterStatusResponse}
import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable, InvalidActorNameException, PoisonPill, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.http.scaladsl.model.HttpResponse
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.analysis.API.{Analyser, BlankAnalyser, LoadExternalAnalyser}
import com.raphtory.core.analysis.Tasks.LiveTasks.{BWindowedLiveAnalysisTask, LiveAnalysisTask, WindowedLiveAnalysisTask}
import com.raphtory.core.analysis.Tasks.RangeTasks.{BWindowedRangeAnalysisTask, RangeAnalysisTask, WindowedRangeAnalysisTask}
import com.raphtory.core.analysis.Tasks.ViewTasks.{BWindowedViewAnalysisTask, ViewAnalysisTask, WindowedViewAnalysisTask}
import com.raphtory.core.model.communication._
import com.twitter.util.Eval

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
case class StartAnalysis()
class AnalysisManager() extends Actor{
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
        val future = currentTasks(jobID) ? RequestResults(jobID:String)
        Await.result(future, timeout.duration) match {
          case results:ResultsForApiPI => sender ! results
        }
      } catch {
        case _: java.util.concurrent.TimeoutException =>
      }
    }
    else sender() ! JobDoesntExist()
  }

  def killJob(jobID: String) = {
    if(currentTasks contains jobID){
      currentTasks(jobID) ! PoisonPill
      currentTasks remove(jobID)
      sender() ! JobKilled()
    }
    else sender()! JobDoesntExist()
  }

  def spawnLiveAnalysisManager(request: LiveAnalysisRequest): Unit = {
    println(s"Live Analysis Task ${request.jobID} received, running ${request.analyserName}")
    try {
      val jobID = request.jobID
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
          context.system.actorOf(Props(new LiveAnalysisTask(managerCount, jobID,args, analyser,repeatTime,eventTime,newAnalyser,analyserFile)), s"LiveAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(Props(new WindowedLiveAnalysisTask(managerCount, jobID,args, analyser,repeatTime,eventTime, request.windowSize,newAnalyser,analyserFile)), s"LiveAnalysisTask__windowed_$jobID")
        case "batched" =>
          context.system.actorOf(Props(new BWindowedLiveAnalysisTask(managerCount, jobID,args, analyser,repeatTime,eventTime, request.windowSet,newAnalyser,analyserFile)), s"LiveAnalysisTask__batchWindowed_$jobID")
      }
      currentTasks put (jobID,ref)
  }
    catch {
      case e:InvalidActorNameException => println("Name non unique please kill other job first")
    }
  }
  def spawnViewAnalysisManager(request: ViewAnalysisRequest): Unit = {
    println(s"View Analysis Task ${request.jobID} received, running ${request.analyserName} at time ${request.timestamp}")
    try{
      val jobID = request.jobID
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
          context.system.actorOf(Props(new ViewAnalysisTask(managerCount,jobID,args,analyser, timestamp,newAnalyser,analyserFile)), s"ViewAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(
            Props(new WindowedViewAnalysisTask(managerCount,jobID,args, analyser, timestamp, request.windowSize,newAnalyser,analyserFile)),
            s"ViewAnalysisTask_windowed_$jobID"
          )
        case "batched" =>
          context.system.actorOf(
            Props(new BWindowedViewAnalysisTask(managerCount,jobID, args,analyser, timestamp, request.windowSet,newAnalyser,analyserFile)),
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
    println(s"Range Analysis Task ${request.jobID} received, running ${request.analyserName}, between ${request.start} and ${request.end} jumping ${request.jump} at a time.")
    try{
      val jobID = request.jobID
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
            .actorOf(Props(new RangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump,newAnalyser,analyserFile)), s"RangeAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(
            Props(new WindowedRangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump, request.windowSize,newAnalyser,analyserFile)),
            s"RangeAnalysisTask_windowed_$jobID"
          )
        case "batched" =>
          context.system.actorOf(
            Props(new BWindowedRangeAnalysisTask(managerCount,jobID,args, analyser, start, end, jump, request.windowSet,newAnalyser,analyserFile)),
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
        val future                    = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest(), false) //ask if the cluster is safe to use
        if(Await.result(future, timeout.duration).asInstanceOf[ClusterStatusResponse].clusterUp) { //if it is
          val future                  = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionCount(), false) //ask how many partitions there are
          managerCount = Await.result(future, timeout.duration).asInstanceOf[PartitionsCountResponse].count //when they respond set the partition manager count to this value
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

  private def getAnalyser(analyserName:String,args:Array[String],rawFile:String): (Boolean,Analyser) ={
    try {
      (false,Class.forName(analyserName).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser])
    } catch {
      case e:ClassNotFoundException => processCompileNewAnalyserRequest(rawFile,args)
    }
  }

  def processCompileNewAnalyserRequest(rawFile:String,args:Array[String]): (Boolean,Analyser) = {
    var analyser: Analyser = new BlankAnalyser(args)
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
