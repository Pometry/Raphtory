package com.raphtory.core.actors.analysismanager

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.analysismanager.AnalysisManager.Message.{JobKilled, _}
import com.raphtory.core.actors.analysismanager.AnalysisManager.State
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message._
import com.raphtory.core.actors.analysismanager.tasks._
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.RaphtoryActor.totalPartitions
import com.raphtory.core.actors.analysismanager.tasks.{LiveAnalysisTask, RangeAnalysisTask, ViewAnalysisTask}
import com.raphtory.core.actors.orchestration.raphtoryleader.WatchDog.Message._
import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser, LoadExternalAnalyser}
import com.raphtory.core.utils.AnalyserUtils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Failure
import scala.util.Success
import scala.util.Try

final case class AnalysisManager() extends RaphtoryActor with ActorLogging with Stash {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart() {
    context.system.scheduler.schedule(Duration(5, SECONDS),Duration(5, SECONDS), self, StartUp)
  }

  override def receive: Receive = init()

  def init(): Receive = {
    case StartUp =>
      mediator ! new DistributedPubSubMediator.Send("/user/WatchDog", AnalysisManagerUp(0)) //ask if the cluster is safe to use
      mediator ! new DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest) //ask if the cluster is safe to use

    case ClusterStatusResponse(clusterUp) =>
      if (clusterUp) context.become(work(State(Map.empty)))
      else context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, StartUp)

    case _: AnalysisRequest => stash()

    case unhandled => log.error(s"unexpected message $unhandled during init stage")
  }

  def work(state: State): Receive = {
    case StartUp => // do nothing as it is ready

    case request: LiveAnalysisRequest =>
      val taskManager = spawnLiveAnalysisManager(request)
      val newState = state.updateCurrentTask(_ ++ taskManager)
      sender() ! ManagingTask(taskManager.get._2)
      context.become(work(newState))

    case request: ViewAnalysisRequest =>
      val taskManager = spawnViewAnalysisManager(request)
      val newState = state.updateCurrentTask(_ ++ taskManager)
      sender() ! ManagingTask(taskManager.get._2)
      context.become(work(newState))

    case request: RangeAnalysisRequest =>
      val taskManager = spawnRangeAnalysisManager(request)
      val newState = state.updateCurrentTask(_ ++ taskManager)
      sender() ! ManagingTask(taskManager.get._2)
      context.become(work(newState))

    case RequestResults(jobId) =>
      state.currentTasks.get(jobId) match {
        case Some(actor) =>
          actor forward RequestResults(jobId)
        case None => sender ! JobDoesntExist
      }

    case req: KillTask =>
      state.currentTasks.get(req.jobId) match {
        case Some(actor) =>
          context.become(work(state.updateCurrentTask(_ - req.jobId)))
          actor forward KillTask
        case None => sender ! JobDoesntExist
      }

    case unhandled => log.error(s"unexpected message $unhandled")
  }

  private def spawnLiveAnalysisManager(request: LiveAnalysisRequest): Option[(String, ActorRef)] = {
    import request._
    val jobId = analyserName + "_" + System.currentTimeMillis()
    log.info(s"Live Analysis Task received, your job ID is $jobId")

    getAnalyser(analyserName, args).map {
      case analyser =>
        val ref = context.system.actorOf(
                Props(LiveAnalysisTask(jobId, args, analyser, getSerialiser(serialiserName), repeatTime, eventTime, windowSet))
                  .withDispatcher("analysis-dispatcher"), s"LiveAnalysisTask_$jobId")
        (jobId, ref)
    }
  }

  private def spawnViewAnalysisManager(request: ViewAnalysisRequest): Option[(String, ActorRef)] = {
    import request._
    val jobId = analyserName + "_" + System.currentTimeMillis()
    log.info(s"View Analysis Task received, your job ID is $jobId")
    getAnalyser(analyserName, args).map {
      case analyser =>
        val ref =
          context.system.actorOf(
                  Props(ViewAnalysisTask(jobId, args, analyser, getSerialiser(serialiserName), timestamp, windowSet))
                    .withDispatcher("analysis-dispatcher"), s"ViewAnalysisTask_$jobId")
        (jobId, ref)
    }
  }

  private def spawnRangeAnalysisManager(request: RangeAnalysisRequest): Option[(String, ActorRef)] = {
    import request._
    val jobId = analyserName + "_" + System.currentTimeMillis()
    log.info(s"Range Analysis Task received, your job ID is $jobId, running $analyserName, between $start and $end jumping $jump at a time.")
    getAnalyser(analyserName, args).map {
      case analyser =>
        val ref = context.system.actorOf(
                    Props(RangeAnalysisTask(jobId, args, analyser, getSerialiser(serialiserName),start, end, jump, windowSet))
                    .withDispatcher("analysis-dispatcher"), s"RangeAnalysisTask_$jobId")
        (jobId, ref)
    }
  }

  private def getAnalyser(
      analyserName: String,
      args: Array[String]): Option[Analyser[Any]] = {
    val tryExist = loadPredefinedAnalyser(analyserName, args)
    tryExist match {
      case Success(analyser) => Some(analyser)
      case Failure(_)        => None
    }
  }

  private def getSerialiser(serialiserName: String):  AggregateSerialiser =
    Class.forName(serialiserName).getConstructor().newInstance().asInstanceOf[AggregateSerialiser]


}

object AnalysisManager {
  private case class State(currentTasks: Map[String, ActorRef]) {
    def updateCurrentTask(f: Map[String, ActorRef] => Map[String, ActorRef]): State =
      copy(currentTasks = f(currentTasks))
  }
  object Message {
    case class  RequestResults(jobId: String)
    case class  KillTask(jobId: String)
    case class  ResultsForApiPI(results: Array[String])
    case class  ManagingTask(actor:ActorRef)
    case class  TaskFinished(result:Boolean)
    case object StartUp
    case object JobKilled
    case object JobDoesntExist
    case object AreYouFinished
    case object JobFailed
  }
}
