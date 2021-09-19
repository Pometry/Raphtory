package com.raphtory.core.actors.partitionmanager.workers

import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.analysismanager.tasks.AnalysisTask.Message._
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.graph.GraphPartition
import com.raphtory.core.utils.AnalyserUtils

import scala.util.Failure
import scala.util.Success


case class ViewJob(jobID: String, timestamp: Long, window: Long)

final case class ReaderWorker(partition: Int, storage: GraphPartition)
        extends RaphtoryActor {
  private val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit =
    log.debug(s"Reader $partition is starting.")

  override def receive: Receive = work()

  private def work(): Receive = {
    case CompileNewAnalyser(jobId, analyser, args) =>
      AnalyserUtils.compileNewAnalyser(analyser, args.toArray) match {
        case Success(analyser) =>
          val subtaskWorker = buildSubtaskWorker(jobId, analyser,sender())
        case Failure(e) =>
          sender ! FailedToCompile(e.getStackTrace.toString)
          log.error("fail to compile new analyser: " + e.getMessage)
      }

    case LoadPredefinedAnalyser(jobId, className, args) =>
      AnalyserUtils.loadPredefinedAnalyser(className, args.toArray) match {
        case Success(analyser) =>
          val subtaskWorker = buildSubtaskWorker(jobId, analyser,sender())

        case Failure(e) =>
          sender ! FailedToCompile(e.getStackTrace.toString)
          log.error("fail to compile predefined analyser: " + e.getMessage)

      }

    case TimeCheck =>
      log.debug(s"Reader [$partition] received TimeCheck.")
      sender ! TimeResponse(storage.windowTime)

    case unhandled =>
      log.error(s"Reader [$partition] received unknown [$unhandled].")
  }
  private def buildSubtaskWorker(jobID: String, analyser: Analyser[Any], taskRef:ActorRef): ActorRef =
    context.system.actorOf(
            Props(AnalysisSubtaskWorker(partition, storage, analyser, jobID,taskRef))
              .withDispatcher("reader-dispatcher"),
            s"read_${partition}_$jobID"
    )
}
