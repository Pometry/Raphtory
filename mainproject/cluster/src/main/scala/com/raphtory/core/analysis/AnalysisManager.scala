package com.raphtory.core.analysis

import com.raphtory.core.model.communication.{ClusterStatusRequest, ClusterStatusResponse}
import akka.actor.{Actor, ActorSystem, Cancellable, InvalidActorNameException, Props}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.LiveTasks.{BWindowedLiveAnalysisTask, LiveAnalysisTask, WindowedLiveAnalysisTask}
import com.raphtory.core.analysis.Tasks.RangeTasks.{BWindowedRangeAnalysisTask, RangeAnalysisTask, WindowedRangeAnalysisTask}
import com.raphtory.core.analysis.Tasks.ViewTasks.{BWindowedViewAnalysisTask, ViewAnalysisTask, WindowedViewAnalysisTask}
import com.raphtory.core.model.communication._

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.language.postfixOps
case class StartAnalysis()
class AnalysisManager() extends Actor{

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  val debug = System.getenv().getOrDefault("DEBUG", "false").trim.toBoolean

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
  }

  def spawnLiveAnalysisManager(request: LiveAnalysisRequest): Unit = {
    println(s"Live Analysis Task ${request.jobID} received, running ${request.analyserName}")
    try {
      val jobID = request.jobID
      val analyser = Class.forName(request.analyserName).newInstance().asInstanceOf[Analyser]
      val args = request.args
      request.windowType match {
        case "false" =>
          context.system.actorOf(Props(new LiveAnalysisTask(managerCount, jobID,args, analyser)), s"LiveAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(Props(new WindowedLiveAnalysisTask(managerCount, jobID,args, analyser, request.windowSize)), s"LiveAnalysisTask__windowed_$jobID")
        case "batched" =>
          context.system.actorOf(Props(new BWindowedLiveAnalysisTask(managerCount, jobID,args, analyser, request.windowSet)), s"LiveAnalysisTask__batchWindowed_$jobID")
      }
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
      val analyser = Class.forName(request.analyserName).newInstance().asInstanceOf[Analyser]
      val args = request.args
      request.windowType match {
        case "false" =>
          context.system.actorOf(Props(new ViewAnalysisTask(managerCount,jobID,args,analyser, timestamp)), s"ViewAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(
            Props(new WindowedViewAnalysisTask(managerCount,jobID,args, analyser, timestamp, request.windowSize)),
            s"ViewAnalysisTask_windowed_$jobID"
          )
        case "batched" =>
          context.system.actorOf(
            Props(new BWindowedViewAnalysisTask(managerCount,jobID, args,analyser, timestamp, request.windowSet)),
            s"ViewAnalysisTask_batchWindowed_$jobID"
          )
      }
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
      val analyser = Class.forName(request.analyserName).getConstructor(classOf[Array[String]]).newInstance(args).asInstanceOf[Analyser]
      request.windowType match {
        case "false" =>
          context.system
            .actorOf(Props(new RangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump)), s"RangeAnalysisTask_$jobID")
        case "true" =>
          context.system.actorOf(
            Props(new WindowedRangeAnalysisTask(managerCount,jobID, args,analyser, start, end, jump, request.windowSize)),
            s"RangeAnalysisTask_windowed_$jobID"
          )
        case "batched" =>
          context.system.actorOf(
            Props(new BWindowedRangeAnalysisTask(managerCount,jobID,args, analyser, start, end, jump, request.windowSet)),
            s"RangeAnalysisTask_batchWindowed_$jobID"
          )
      }
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
}
