package com.raphtory.core.actors.PartitionManager.Workers

import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask.Message._
import com.raphtory.core.actors.ClusterManagement.RaphtoryReplicator.Message.UpdatedCounter
import com.raphtory.core.actors.PartitionManager.Workers.AnalysisSubtaskWorker.State
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.GraphLens
import com.raphtory.core.analysis.api.Analyser
import com.raphtory.core.model.EntityStorage
import com.raphtory.core.model.communication.VertexMessage
import kamon.Kamon

import scala.util.Failure
import scala.util.Success
import scala.util.Try

final case class AnalysisSubtaskWorker(
    initManagerCount: Int,
    managerId: Int,
    workerId: Int,
    storage: EntityStorage,
    analyzer: Analyser[Any],
    jobId: String
) extends RaphtoryActor {
  private val mediator: ActorRef = DistributedPubSub(context.system).mediator

  override def preStart(): Unit =
    log.debug(
            s"AnalysisSubtaskWorker for Job [$jobId] belonging to ReaderWorker [$workerId] Reader [$managerId] is being started."
    )

  override def receive: Receive = work(State(0, 0, initManagerCount))

  private def work(state: State): Receive = {
    case StartSubtask(_, timestamp, window) =>
      log.debug(s"Job [$jobId] belonging to ReaderWorker [$workerId] Reader [$managerId] is SetupTaskWorker.")
      val beforeTime = System.currentTimeMillis()
      val initStep   = 0
      val graphLens  = GraphLens(jobId, timestamp, window, initStep, workerId, storage)
      analyzer.sysSetup(graphLens, workerId)
      analyzer.setup()
      val messages = analyzer.view.getAndCleanMessages()
      messages.foreach(m => mediator ! new DistributedPubSubMediator.Send(getReader(m.vertexId, state.managerCount), m))
      sender ! Ready(messages.size)
      stepMetric(analyzer).update(System.currentTimeMillis() - beforeTime)
      context.become(work(state.updateSentMessageCount(_ + messages.size)))

    case _: CheckMessages =>
      log.debug(s"Job [$jobId] belonging to ReaderWorker [$workerId] Reader [$managerId] receives CheckMessages.")
      Kamon
        .gauge("Raphtory_Analysis_Messages_Received")
        .withTag("actor", s"Reader_$managerId")
        .withTag("ID", workerId)
        .withTag("jobID", jobId)
        .withTag("Timestamp", analyzer.view.timestamp)
        .withTag("Superstep", analyzer.view.superStep)
        .update(state.receivedMessageCount)

      Kamon
        .gauge("Raphtory_Analysis_Messages_Sent")
        .withTag("actor", s"Reader_$managerId")
        .withTag("ID", workerId)
        .withTag("ID", workerId)
        .withTag("jobID", jobId)
        .withTag("Timestamp", analyzer.view.timestamp)
        .withTag("Superstep", analyzer.view.superStep)
        .update(state.sentMessageCount)

      sender ! MessagesReceived(state.receivedMessageCount, state.sentMessageCount)

    case _: NextStep =>
      log.debug(s"Job [$jobId] belonging to ReaderWorker [$workerId] Reader [$managerId] receives NextStep.")
      val beforeTime = System.currentTimeMillis()
      analyzer.view.nextStep()
      Try(analyzer.analyse()) match {
        case Success(_) =>
          val messages = analyzer.view.getAndCleanMessages()
          messages.foreach { m =>
            mediator ! new DistributedPubSubMediator.Send(getReader(m.vertexId, state.managerCount), m)
          }
          sender ! EndStep(analyzer.view.superStep, messages.size, analyzer.view.checkVotes())

        case Failure(e) => log.error(s"Failed to run nextStep due to [$e].")
      }
      stepMetric(analyzer).update(System.currentTimeMillis() - beforeTime)

    case _: Finish =>
      log.debug(s"Job [$jobId] belonging to ReaderWorker [$workerId] Reader [$managerId] receives Finish.")
      Try(analyzer.returnResults()) match {
        case Success(result) => sender ! ReturnResults(result)
        case Failure(e)      => log.error(s"Failed to run nextStep due to [$e].")
      }

    case msg: VertexMessage =>
      log.debug(s"Job [$jobId] belonging to ReaderWorker [$workerId] Reader [$managerId] receives VertexMessage.")
      analyzer.view.receiveMessage(msg)
      context.become(work(state.updateReceivedMessageCount(_ + 1)))

    case UpdatedCounter(newValue) =>
      context.become(work(state.copy(managerCount = newValue)))

    case unhandled => log.error(s"Unexpected message [$unhandled].")
  }

  private def stepMetric(analyser: Analyser[Any]) =
    Kamon
      .gauge("Raphtory_Superstep_Time")
      .withTag("Partition", storage.managerID)
      .withTag("Worker", workerId)
      .withTag("JobID", jobId)
      .withTag("timestamp", analyser.view.timestamp)
      .withTag("superstep", analyser.view.superStep)

}

object AnalysisSubtaskWorker {
  private case class State(sentMessageCount: Int, receivedMessageCount: Int, managerCount: Int) {
    def updateSentMessageCount(f: Int => Int): State     = copy(sentMessageCount = f(sentMessageCount))
    def updateReceivedMessageCount(f: Int => Int): State = copy(receivedMessageCount = f(receivedMessageCount))
  }
  object Message {
    case class SetupTaskWorker(timestamp: Long, window: Option[Long])
  }
}
