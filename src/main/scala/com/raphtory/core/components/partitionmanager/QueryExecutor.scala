package com.raphtory.core.components.partitionmanager

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.partitionmanager.QueryExecutor.State
import com.raphtory.core.components.querymanager.QueryHandler.Message.{CreatePerspective, ExecutorEstablished, PerspectiveEstablished}
import com.raphtory.core.implementations
import com.raphtory.core.implementations.objectgraph.messaging.VertexMessageHandler
import com.raphtory.core.model.graph.GraphPartition

case class QueryExecutor(partition: Int, storage: GraphPartition, jobId: String, handlerRef:ActorRef) extends RaphtoryActor {

  private val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit = {
    log.debug(s"Query Executor ${self.path} for Job [$jobId] belonging to Reader [$partition] started.")
    handlerRef ! ExecutorEstablished(partition, self)
  }

  override def receive: Receive = work(State(0, 0))

  private def work(state: State): Receive = {
    case CreatePerspective(neighbours, timestamp, window) =>
      val messageHandler = new VertexMessageHandler(neighbours, jobId)
      val graphLens = implementations.objectgraph.ObjectGraphLens(jobId, timestamp, window, 0, storage, messageHandler)
      context.become(work(state.copy(sentMessageCount = 0, receivedMessageCount = 0)))
      sender ! PerspectiveEstablished
  }

}

object QueryExecutor {
  private case class State(sentMessageCount: Int, receivedMessageCount: Int) {
    def updateReceivedMessageCount(f: Int => Int): State = copy(receivedMessageCount = f(receivedMessageCount))
  }
}