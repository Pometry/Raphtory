package com.raphtory.core.components.partitionmanager

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.analysismanager.tasks.AnalysisTask.Message._
import com.raphtory.core.model.algorithm.Analyser
import com.raphtory.core.model.graph.GraphPartition

import scala.util.{Failure, Success}

final case class Reader(partition: Int, storage: GraphPartition) extends RaphtoryActor {
  private val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit =
    log.debug(s"Reader $partition is starting.")

  override def receive: Receive = work()

  private def work(): Receive = {
    case LoadAnalyser(jobId, className, args) =>
      loadPredefinedAnalyser(className, args.toArray) match {
        case Success(analyser) => buildQueryExecutor(jobId, analyser,sender())
        case Failure(e) => log.error("Analyser Could not be loaded: " + e.getMessage)
      }

    case TimeCheck =>
      log.debug(s"Reader [$partition] received TimeCheck.")
      sender ! TimeResponse(storage.windowTime)

    case unhandled =>
      log.error(s"Reader [$partition] received unknown [$unhandled].")
  }

  private def buildQueryExecutor(jobID: String, analyser: Analyser[Any], taskRef:ActorRef): ActorRef =
    context.system.actorOf(
            Props(QueryExecutor(partition, storage, analyser, jobID,taskRef))
              .withDispatcher("reader-dispatcher"),
            s"read_${partition}_$jobID"
    )
}
