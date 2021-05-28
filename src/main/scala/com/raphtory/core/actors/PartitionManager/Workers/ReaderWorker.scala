package com.raphtory.core.actors.PartitionManager.Workers

import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.Message.KillTask
import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask.Message._
import com.raphtory.core.actors.ClusterManagement.RaphtoryReplicator.Message.UpdatedCounter
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.AnalyserUtils

import scala.util.Failure
import scala.util.Success


case class ViewJob(jobID: String, timestamp: Long, window: Long)

final case class ReaderWorker(initManagerCount: Int, managerId: Int, workerId: Int, storage: EntityStorage)
        extends RaphtoryActor {
  private val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit =
    log.debug(s"ReaderWorker [$workerId] belonging to Reader [$managerId] is being started.")

  override def receive: Receive = work(initManagerCount)

  private def work(managerCount: Int): Receive = {
    case CompileNewAnalyser(jobId, analyser, args) =>
      AnalyserUtils.compileNewAnalyser(analyser, args) match {
        case Success(analyser) =>
          sender() ! AnalyserPresent
          val subtaskWorker = buildSubtaskWorker(jobId, analyser, managerCount)
        case Failure(e) =>
          sender ! FailedToCompile(e.getStackTrace.toString)
          log.error("fail to compile new analyser: " + e.getMessage)
      }

    case LoadPredefinedAnalyser(jobId, className, args) =>
      AnalyserUtils.loadPredefinedAnalyser(className, args) match {
        case Success(analyser) =>
          sender() ! AnalyserPresent
          val subtaskWorker = buildSubtaskWorker(jobId, analyser, managerCount)
        case Failure(e) =>
          sender ! FailedToCompile(e.getStackTrace.toString)
          log.error("fail to compile predefined analyser: " + e.getMessage)

      }

    case TimeCheck =>
      log.debug(s"Reader [$workerId] received TimeCheck.")
      sender ! TimeResponse(storage.windowTime)

    case unhandled =>
      log.error(s"ReaderWorker [$workerId] belonging to Reader [$managerId] received unknown [$unhandled].")
  }
  private def buildSubtaskWorker(jobId: String, analyser: Analyser[Any], managerCount: Int): ActorRef =
    context.system.actorOf(
            Props(AnalysisSubtaskWorker(managerCount, managerId, workerId, storage, analyser, jobId))
              .withDispatcher("reader-dispatcher"),
            s"Manager_${managerId}_reader_${workerId}_analysis_subtask_worker_$jobId"
    )
}
