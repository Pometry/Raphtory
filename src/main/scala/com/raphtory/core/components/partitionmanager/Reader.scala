package com.raphtory.core.components.partitionmanager

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.querymanager.QueryHandler.Message._
import com.raphtory.core.model.algorithm.Analyser
import com.raphtory.core.model.graph.GraphPartition

import scala.util.{Failure, Success}

final case class Reader(partition: Int, storage: GraphPartition) extends RaphtoryActor {


  override def preStart(): Unit =
    log.debug(s"Reader $partition is starting.")

  override def receive: Receive = work()

  private def work(): Receive = {
    case LoadAnalyser(jobId, className, args) =>
      loadPredefinedAnalyser(className, args.toArray) match {
        case Success(analyser) => buildAnalysisExecutor(jobId, analyser,sender())
        case Failure(e) => log.error("Analyser Could not be loaded: " + e.getMessage)
      }

    case EstablishExecutor(jobID) =>  {
      buildQueryExecutor(jobID,sender())
    }

    case TimeCheck =>
      log.debug(s"Reader [$partition] received TimeCheck.")
      sender ! TimeResponse(storage.windowTime)

    case unhandled =>
      log.error(s"Reader [$partition] received unknown [$unhandled].")
  }

  private def buildAnalysisExecutor(jobID: String, analyser: Analyser[Any], taskRef:ActorRef): ActorRef =
    context.system.actorOf(
            Props(AnalyserExecutor(partition, storage, analyser, jobID,taskRef))
              .withDispatcher("reader-dispatcher"),
            s"read_${partition}_$jobID"
    )

  private def buildQueryExecutor(jobID: String, handlerRef:ActorRef): ActorRef =
    context.system.actorOf(
      Props(QueryExecutor(partition, storage, jobID,handlerRef))
        .withDispatcher("reader-dispatcher"),
      s"read_${partition}_$jobID"
    )
}
