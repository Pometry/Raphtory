package com.raphtory.core.components.partitionmanager

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.partitionmanager.QueryExecutor.State
import com.raphtory.core.components.querymanager.QueryHandler.Message.{CheckMessages, CreatePerspective, ExecutorEstablished, GraphFunctionComplete, MetaDataSet, PerspectiveEstablished, SetMetaData, TableBuilt, TableFunctionComplete}
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, EndQuery}
import com.raphtory.core.implementations
import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.implementations.objectgraph.messaging.VertexMessageHandler
import com.raphtory.core.model.algorithm.{Iterate, Select, Step, TableFilter, VertexFilter, WriteTo}
import com.raphtory.core.model.graph.{GraphPartition, VertexMessage}
import com.raphtory.core.model.graph.visitor.Vertex

import java.io.File
import scala.io.Source

case class QueryExecutor(partition: Int, storage: GraphPartition, jobID: String, handlerRef:ActorRef) extends RaphtoryActor {

  override def preStart(): Unit = {
    log.debug(s"Query Executor ${self.path} for Job [$jobID] belonging to Reader [$partition] started.")
    handlerRef ! ExecutorEstablished(partition, self)
  }

  override def receive: Receive = work(State(null,0, 0,false))

  private def work(state: State): Receive = withDefaultMessageHandler("work") {

    case msg: VertexMessage =>
      log.debug(s"Job [$jobID] belonging to Reader [$partition] receives VertexMessage.")
      state.graphLens.receiveMessage(msg)
      context.become(work(state.updateReceivedMessageCount(_ + 1)))

    case CreatePerspective(neighbours, timestamp, window) =>
      context.become(work(state.copy(
        graphLens = ObjectGraphLens(jobID, timestamp, window, 0, storage, VertexMessageHandler(neighbours)),
        sentMessageCount = 0,
        receivedMessageCount = 0)
      ))
      sender ! PerspectiveEstablished(state.graphLens.getSize())

    case SetMetaData(vertices) =>
      state.graphLens.setGraphSize(vertices)
      sender() ! MetaDataSet

    case Step(f) =>
      //log.info(s"Partition $partition have been asked to do a Step operation.")
      state.graphLens.nextStep()
      state.graphLens.runGraphFunction(f)
      val sentMessages = state.graphLens.getMessageHandler().getCount()
      sender() ! GraphFunctionComplete(sentMessages,state.receivedMessageCount)
      context.become(work(state.copy(sentMessageCount=sentMessages)))

    case Iterate(f,iterations) =>
      //log.info(s"Partition $partition have been asked to do an Iterate operation. There are $iterations Iterations remaining")
      state.graphLens.nextStep()
      state.graphLens.runMessagedGraphFunction(f)
      val sentMessages = state.graphLens.getMessageHandler().getCount()
      sender() ! GraphFunctionComplete(state.receivedMessageCount,sentMessages,state.graphLens.checkVotes())
      context.become(work(state.copy(votedToHalt = state.graphLens.checkVotes(),sentMessageCount = sentMessages)))

    case VertexFilter(f) =>
      //log.info(s"Partition $partition have been asked to do a Graph Filter operation. Not yet implemented")
      sender() ! GraphFunctionComplete(0,0)

    case Select(f) =>
      //log.info(s"Partition $partition have been asked to do a Select operation.")
      state.graphLens.executeSelect(f)
      sender() ! TableBuilt


    case TableFilter(f) =>
      //log.info(s"Partition $partition have been asked to do a Table Filter operation.")
      state.graphLens.filteredTable(f)
      sender() ! TableFunctionComplete


    case WriteTo(address) =>
      //log.info(s"Partition $partition have been asked to do a Table WriteTo operation.")
      val dir = new File(s"$address/$jobID")
      if(!dir.exists())
        dir.mkdirs()
      state.graphLens.getDataTable().foreach(row=>{
        state.graphLens.window match {
          case Some(window) =>
            reflect.io.File(s"$address/$jobID/partition-$partition").appendAll(s"${state.graphLens.timestamp},$window,"+row.getValues().mkString(",")+"\n")
          case None =>
            reflect.io.File(s"$address/$jobID/partition-$partition").appendAll(s"${state.graphLens.timestamp},"+row.getValues().mkString(",")+"\n")

        }
      })

      sender() ! TableFunctionComplete

    case _: CheckMessages =>
      log.debug(s"Job [$jobID] belonging to Reader [$partition] receives CheckMessages.")
      sender ! GraphFunctionComplete(state.receivedMessageCount, state.sentMessageCount,state.votedToHalt)
  }

  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case req: EndQuery =>
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