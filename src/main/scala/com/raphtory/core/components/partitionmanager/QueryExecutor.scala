package com.raphtory.core.components.partitionmanager

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.partitionmanager.QueryExecutor.State
import com.raphtory.core.components.querymanager.QueryHandler.Message.{CheckMessages, CreatePerspective, ExecutorEstablished, GraphFunctionComplete, PerspectiveEstablished, TableBuilt, TableFunctionComplete}
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, KillTask}
import com.raphtory.core.implementations
import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.implementations.objectgraph.messaging.VertexMessageHandler
import com.raphtory.core.model.algorithm.{Iterate, Select, Step, TableFilter, VertexFilter, WriteTo}
import com.raphtory.core.model.graph.{GraphPartition, VertexMessage}
import com.raphtory.core.model.graph.visitor.Vertex

case class QueryExecutor(partition: Int, storage: GraphPartition, jobID: String, handlerRef:ActorRef) extends RaphtoryActor {

  override def preStart(): Unit = {
    log.debug(s"Query Executor ${self.path} for Job [$jobID] belonging to Reader [$partition] started.")
    handlerRef ! ExecutorEstablished(partition, self)
  }

  override def receive: Receive = work(State(null,0, 0,false))

  private def work(state: State): Receive = withDefaultMessageHandler("work") {
    case CreatePerspective(neighbours, timestamp, window) =>
      context.become(work(state.copy(
        graphLens = ObjectGraphLens(jobID, timestamp, window, 0, storage, VertexMessageHandler(neighbours)),
        sentMessageCount = 0,
        receivedMessageCount = 0)
      ))
      sender ! PerspectiveEstablished

    case Step(f) =>
      log.info(s"Partition $partition have been asked to do a Step operation.")
      state.graphLens.nextStep()
      state.graphLens.runGraphFunction(f)
      val sentMessages = state.graphLens.getMessageHandler().getCount()
      sender() ! GraphFunctionComplete(sentMessages,state.receivedMessageCount)
      context.become(work(state.copy(sentMessageCount=sentMessages)))

    case Iterate(f,iterations) =>
      log.info(s"Partition $partition have been asked to do an Iterate operation. There are $iterations Iterations remaining")
      state.graphLens.nextStep()
      state.graphLens.runMessagedGraphFunction(f)
      val sentMessages = state.graphLens.getMessageHandler().getCount()
      sender() ! GraphFunctionComplete(sentMessages,state.receivedMessageCount,state.graphLens.checkVotes())
      context.become(work(state.copy(votedToHalt = state.graphLens.checkVotes(),sentMessageCount = sentMessages)))

    case VertexFilter(f) =>
      log.info(s"Partition $partition have been asked to do a Graph Filter operation.")
      sender() ! GraphFunctionComplete(0,0)

    case Select(f) =>
      log.info(s"Partition $partition have been asked to do a Select operation.")
      sender() ! TableBuilt


    case TableFilter(f) =>
      log.info(s"Partition $partition have been asked to do a Table Filter operation.")
      sender() ! TableFunctionComplete


    case WriteTo(f) =>
      log.info(s"Partition $partition have been asked to do a Table WriteTo operation.")
      sender() ! TableFunctionComplete


    case msg: VertexMessage =>
      log.debug(s"Job [$jobID] belonging to Reader [$partition] receives VertexMessage.")
      state.graphLens.receiveMessage(msg)
      context.become(work(state.updateReceivedMessageCount(_ + 1)))

    case _: CheckMessages =>
      log.info(s"Job [$jobID] belonging to Reader [$partition] receives CheckMessages.")
      sender ! GraphFunctionComplete(state.receivedMessageCount, state.sentMessageCount,state.votedToHalt)
  }

  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case req: KillTask =>
      context.stop(self)
      log.info(s"Executor for Partition $partition Job $jobID has been terminated")
    case unhandled     => log.error(s"Not handled message in $description: " + unhandled)
  }


}

object QueryExecutor {
  private case class State(graphLens: ObjectGraphLens,sentMessageCount: Int, receivedMessageCount: Int,votedToHalt:Boolean) {
    def updateReceivedMessageCount(f: Int => Int): State = copy(receivedMessageCount = f(receivedMessageCount))
  }
}