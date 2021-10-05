package com.raphtory.core.components.partitionmanager

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.management.RaphtoryActor
import com.raphtory.core.components.querymanager.QueryManager.Message.{JobFailed, KillTask}
import com.raphtory.core.components.querymanager.QueryHandler.Message._
import com.raphtory.core.components.partitionmanager.AnalyserExecutor.State
import com.raphtory.core.implementations.objectgraph.ObjectGraphLens
import com.raphtory.core.model.algorithm.Analyser
import com.raphtory.core.implementations.objectgraph.messaging.VertexMessageHandler
import com.raphtory.core.model.graph.{GraphPartition, VertexMessage}
import com.raphtory.core.implementations

import scala.util.{Failure, Success, Try}

final case class AnalyserExecutor(partition: Int, storage: GraphPartition, analyzer: Analyser[Any], jobId: String, taskManager:ActorRef) extends RaphtoryActor {

  override def preStart(): Unit = {
    log.debug(s"AnalysisSubtaskWorker ${self.path} for Job [$jobId] belonging to Reader [$partition] is being started.")
    taskManager ! ExecutorEstablished(partition, self)
  }

  override def postStop(): Unit = log.info(s"Worker for $jobId Killed")

  override def receive: Receive = work(State(0, 0))

  private def work(state: State): Receive = {
    case CreatePerspective(neighbours, timestamp, window) =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] is SetupTaskWorker.")
      val initStep = 0
      val messageHandler = new VertexMessageHandler(neighbours)
      val graphLens = implementations.objectgraph.ObjectGraphLens(jobId, timestamp, window, initStep, storage, messageHandler)
      analyzer.sysSetup(graphLens, messageHandler)
      context.become(work(state.copy(sentMessageCount = 0, receivedMessageCount = 0)))
      sender ! PerspectiveEstablished

    case _: StartSubtask =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] is StartSubtask.")
      analyzer.setup()
      val messagesSent = analyzer.messageHandler.getCountandReset()
      sender ! Ready(messagesSent)
      context.become(work(state.copy(sentMessageCount = messagesSent)))

    case _: CheckMessages =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] receives CheckMessages.")
      sender ! GraphFunctionComplete(state.receivedMessageCount, state.sentMessageCount)

    case _: SetupNextStep =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] receives SetupNextStep.")
      Try(analyzer.view.nextStep()) match {
        case Success(_) =>
          context.become(work(state.copy(sentMessageCount = 0, receivedMessageCount = 0)))
          sender ! SetupNextStepDone
        case Failure(e) => {
          log.error(s"Failed to run setup due to [${e.getStackTrace.mkString("\n")}].")
          sender ! JobFailed
        }
      }


    case _: StartNextStep =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] receives StartNextStep.")
      Try(analyzer.analyse()) match {
        case Success(_) =>
          val messageCount = analyzer.messageHandler.getCountandReset()
          sender ! EndStep(analyzer.view.superStep, messageCount, analyzer.view.checkVotes())
          context.become(work(state.copy(sentMessageCount = messageCount)))

        case Failure(e) => {
          log.error(s"Failed to run nextStep due to [${e.getStackTrace.mkString("\n")}].")
          sender ! JobFailed
        }
      }

    case _: Finish =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] receives Finish.")
      Try(analyzer.returnResults()) match {
        case Success(result) => sender ! ReturnResults(result)
        case Failure(e) => {
          log.error(s"Failed to run nextStep due to [${e.getStackTrace.mkString("\n")}].")
          sender ! JobFailed
        }

      }

    case msg: VertexMessage =>
      log.debug(s"Job [$jobId] belonging to Reader [$partition] receives VertexMessage.")
      analyzer.view.receiveMessage(msg)
      context.become(work(state.updateReceivedMessageCount(_ + 1)))

    case KillTask(jobID) => self ! PoisonPill

    case unhandled => log.error(s"Unexpected message [$unhandled].")


  }
}

object AnalyserExecutor {
  private case class State(sentMessageCount: Int, receivedMessageCount: Int) {
    def updateReceivedMessageCount(f: Int => Int): State = copy(receivedMessageCount = f(receivedMessageCount))
  }

}
