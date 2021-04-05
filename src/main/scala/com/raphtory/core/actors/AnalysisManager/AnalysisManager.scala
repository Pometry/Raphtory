package com.raphtory.core.actors.AnalysisManager

import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Stash
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.Message.{JobKilled, _}
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.State
import com.raphtory.core.actors.AnalysisManager.AnalysisRestApi.message._
import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask.Message.FailedToCompile
import com.raphtory.core.actors.AnalysisManager.Tasks.LiveTasks._
import com.raphtory.core.actors.AnalysisManager.Tasks.RangeTasks._
import com.raphtory.core.actors.AnalysisManager.Tasks.ViewTasks._
import com.raphtory.core.actors.ClusterManagement.WatchDog.Message._
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.analysis.api.LoadExternalAnalyser

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
    context.system.scheduler.scheduleOnce(Duration(5, SECONDS), self, StartUp)
  }

  override def receive: Receive = init()

  def init(): Receive = {
    case StartUp =>
      mediator ! new DistributedPubSubMediator.Send(
              "/user/WatchDog",
              ClusterStatusRequest
      ) //ask if the cluster is safe to use

    case ClusterStatusResponse(clusterUp, _, _) =>
      if (clusterUp) mediator ! new DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionCount)
      else context.system.scheduler.scheduleOnce(Duration(1, SECONDS), self, StartUp)

    case PartitionsCount(count) =>
      context.become(work(State(count, Map.empty)))
      unstashAll()

    case _: AnalysisRequest => stash()

    case unhandled => log.error(s"unexpected message $unhandled during init stage")
  }

  def work(state: State): Receive = {
    case StartUp => // do nothing as it is ready

    case PartitionsCount(newValue) =>
      context.become(work(state.copy(managerCount = newValue)))

    case request: LiveAnalysisRequest =>
      val newState = state.updateCurrentTask(_ ++ spawnLiveAnalysisManager(state.managerCount, request))
      context.become(work(newState))

    case request: ViewAnalysisRequest =>
      val newState = state.updateCurrentTask(_ ++ spawnViewAnalysisManager(state.managerCount, request))
      context.become(work(newState))

    case request: RangeAnalysisRequest =>
      val newState = state.updateCurrentTask(_ ++ spawnRangeAnalysisManager(state.managerCount, request))
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

  private def spawnLiveAnalysisManager(managerCount: Int, request: LiveAnalysisRequest): Option[(String, ActorRef)] = {
    import request._
    val jobId = analyserName + "_" + System.currentTimeMillis()
    log.info(s"Live Analysis Task received, your job ID is $jobId")

    getAnalyser(analyserName, args, request.rawFile).map {
      case (newAnalyser, analyser) =>
        val ref = context.system.actorOf(
                Props(
                        LiveAnalysisTask(
                                managerCount,
                                jobId,
                                args,
                                analyser,
                                repeatTime,
                                eventTime,
                                windowSet,
                                newAnalyser,
                                rawFile
                        )
                ).withDispatcher("analysis-dispatcher"),
                s"LiveAnalysisTask_$jobId"
        )

        (jobId, ref)
    }
  }

  private def spawnViewAnalysisManager(managerCount: Int, request: ViewAnalysisRequest): Option[(String, ActorRef)] = {
    import request._
    val jobId = analyserName + "_" + System.currentTimeMillis()
    log.info(s"View Analysis Task received, your job ID is $jobId")
    getAnalyser(analyserName, args, rawFile).map {
      case (newAnalyser, analyser) =>
        val ref =
          context.system.actorOf(
                  Props(
                          ViewAnalysisTask(
                                  managerCount,
                                  jobId,
                                  args,
                                  analyser,
                                  timestamp,
                                  windowSet,
                                  newAnalyser,
                                  rawFile
                          )
                  ).withDispatcher("analysis-dispatcher"),
                  s"ViewAnalysisTask_$jobId"
          )
        (jobId, ref)
    }
  }

  private def spawnRangeAnalysisManager(
      managerCount: Int,
      request: RangeAnalysisRequest
  ): Option[(String, ActorRef)] = {
    import request._
    val jobId = analyserName + "_" + System.currentTimeMillis()
    log.info(
            s"Range Analysis Task received, your job ID is $jobId, running $analyserName, between $start and $end jumping $jump at a time."
    )
    getAnalyser(analyserName, args, rawFile).map {
      case (newAnalyser, analyser) =>
        val ref =
          context.system.actorOf(
                  Props(
                          RangeAnalysisTask(
                                  managerCount,
                                  jobId,
                                  args,
                                  analyser,
                                  start,
                                  end,
                                  jump,
                                  windowSet,
                                  newAnalyser,
                                  rawFile
                          )
                  ).withDispatcher("analysis-dispatcher"),
                  s"RangeAnalysisTask_$jobId"
          )
        (jobId, ref)
    }
  }

  private def getAnalyser(
      analyserName: String,
      args: Array[String],
      rawFile: String
  ): Option[(Boolean, Analyser[Any])] = {
    val tryExist = Try(
            Class
              .forName(analyserName)
              .getConstructor(classOf[Array[String]])
              .newInstance(args)
              .asInstanceOf[Analyser[Any]]
    ).orElse(Try(Class.forName(analyserName).getConstructor().newInstance().asInstanceOf[Analyser[Any]]))

    tryExist match {
      case Success(analyser) => Some((false, analyser))
      case Failure(_)        => compileNewAnalyser(rawFile, args).map((true, _))
    }
  }

  private def compileNewAnalyser(rawFile: String, args: Array[String]): Option[Analyser[Any]] =
    Try(LoadExternalAnalyser(rawFile, args).newAnalyser) match {
      case Success(analyser) => Some(analyser)
      case Failure(e) =>
        sender ! FailedToCompile(e.getStackTrace.mkString(","))
        log.info(e.getMessage)
        None
    }
}

object AnalysisManager {
  private case class State(managerCount: Int, currentTasks: Map[String, ActorRef]) {
    def updateCurrentTask(f: Map[String, ActorRef] => Map[String, ActorRef]): State =
      copy(currentTasks = f(currentTasks))
  }
  object Message {
    case object StartUp
    case class RequestResults(jobId: String)
    case class KillTask(jobId: String)
    case object JobKilled
    case class ResultsForApiPI(results: Array[String])
    case object JobDoesntExist
  }
}
