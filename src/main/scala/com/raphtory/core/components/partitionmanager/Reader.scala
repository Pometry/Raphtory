package com.raphtory.core.components.partitionmanager

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.querymanager.QueryHandler.Message._
import com.raphtory.core.model.graph.GraphPartition

import scala.util.{Failure, Success}

final class Reader(partition: Int, storage: GraphPartition) extends RaphtoryActor {

  override def preStart(): Unit = log.debug(s"Reader $partition is starting.")
  override def receive: Receive = work()

  private def work(): Receive = {
    case EstablishExecutor(jobID) =>
      buildQueryExecutor(jobID,sender())

    case unhandled =>
      log.error(s"Reader [$partition] received unknown [$unhandled].")
  }

  private def buildQueryExecutor(jobID: String, handlerRef:ActorRef): ActorRef =
    context.system.actorOf(
      Props(QueryExecutor(partition, storage, jobID,handlerRef))
        .withDispatcher("reader-dispatcher"),
      s"read_${partition}_$jobID"
    )
}
