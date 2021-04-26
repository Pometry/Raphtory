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
import com.raphtory.core.analysis.api.LoadExternalAnalyser
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.AnalyserUtils

import scala.collection.concurrent.TrieMap
import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class ViewJob(jobID: String, timestamp: Long, window: Long)

final case class ReaderWorker(initManagerCount: Int, managerId: Int, workerId: Int, storage: EntityStorage)
        extends RaphtoryActor {
  private val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit =
    log.debug(s"ReaderWorker [$workerId] belonging to Reader [$managerId] is being started.")

  override def receive: Receive = work(Map.empty, initManagerCount)

  private def work(subtaskWorkerMap: Map[String, ActorRef], managerCount: Int): Receive = {
    case CompileNewAnalyser(jobId, analyser, args) =>
      AnalyserUtils.compileNewAnalyser(analyser, args) match {
        case Success(analyser) =>
          sender() ! AnalyserPresent
          val subtaskWorker = buildSubtaskWorker(jobId, analyser, managerCount)
          context.become(work(subtaskWorkerMap + (jobId -> subtaskWorker), managerCount))
        case Failure(e) =>
          sender ! FailedToCompile(e.getStackTrace.toString)
          log.error("fail to compile new analyser: " + e.getMessage)
      }

    case LoadPredefinedAnalyser(jobId, className, args) =>
      AnalyserUtils.loadPredefinedAnalyser(className, args) match {
        case Success(analyser) =>
          sender() ! AnalyserPresent
          val subtaskWorker = buildSubtaskWorker(jobId, analyser, managerCount)
          context.become(work(subtaskWorkerMap + (jobId -> subtaskWorker), managerCount))
        case Failure(e) =>
          sender ! FailedToCompile(e.getStackTrace.toString)
          log.error("fail to compile predefined analyser: " + e.getMessage)
      }

    case TimeCheck =>
      log.debug(s"Reader [$workerId] received TimeCheck.")
      sender ! TimeResponse(storage.windowTime)

    case req: StartSubtask  => forwardMsgToSubtaskWorker(req.jobId, subtaskWorkerMap, req)
    case req: CheckMessages => forwardMsgToSubtaskWorker(req.jobId, subtaskWorkerMap, req)
    case req: NextStep      => forwardMsgToSubtaskWorker(req.jobId, subtaskWorkerMap, req)
    case req: Finish        => forwardMsgToSubtaskWorker(req.jobId, subtaskWorkerMap, req)
    case req: VertexMessage => forwardMsgToSubtaskWorker(req.jobId, subtaskWorkerMap, req)

    case req: UpdatedCounter =>
      subtaskWorkerMap.values.foreach(_ forward UpdatedCounter)
      context.become(work(subtaskWorkerMap, req.newValue))

    case KillTask(jobId) =>
      subtaskWorkerMap.get(jobId).foreach(context.stop)
      context.become(work(subtaskWorkerMap - jobId, managerCount))
    case unhandled =>
      log.error(s"ReaderWorker [$workerId] belonging to Reader [$managerId] received unknown [$unhandled].")
  }

  private def buildSubtaskWorker(jobId: String, analyser: Analyser[Any], managerCount: Int): ActorRef =
    context.system.actorOf(
            Props(AnalysisSubtaskWorker(managerCount, managerId, workerId, storage, analyser, jobId))
              .withDispatcher("reader-dispatcher"),
            s"Manager_${managerId}_reader_${workerId}_analysis_subtask_worker_$jobId"
    )

  private def  forwardMsgToSubtaskWorker[T](jobId: String, map: Map[String, ActorRef], msg: T): Unit =
    map.get(jobId) match {
      case Some(worker) => worker forward msg
      case None         => log.error(s"unexpected sate: $jobId does not have actor for $msg")
    }
}
