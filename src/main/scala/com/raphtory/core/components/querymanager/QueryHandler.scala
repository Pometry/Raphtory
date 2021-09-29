package com.raphtory.core.components.querymanager

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.RaphtoryActor.totalPartitions
import com.raphtory.core.components.querymanager.QueryHandler.Message.{EstablishExecutor, ExecutorEstablished, StartAnalysis, TimeCheck}
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, JobKilled, KillTask, TaskFinished}
import com.raphtory.core.model.algorithm.GraphAlgorithm

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS}

abstract class QueryHandler(jobID:String,algorithm:GraphAlgorithm) extends RaphtoryActor{
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  private val workerList = mutable.Map[Int,ActorRef]()

  private var monitor:ActorRef = _

  protected def buildSubTaskController(latestTimestamp: Long): PerspectiveController
  override def preStart() = context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis)
  override def receive: Receive = spawnExecutors(0)

  private def spawnExecutors(readyCount: Int): Receive = withDefaultMessageHandler("spawn executors") {
    case StartAnalysis => messageToAllReaders(EstablishExecutor(jobID))
    case ExecutorEstablished(workerID,actor) => //analyser confirmed to be present within workers, send setup request to workers
      workerList += ((workerID,actor))
      if (readyCount + 1 == totalPartitions) {
        println("all executors spawned")
        //messageToAllReaders(TimeCheck)
        //context.become(checkTime(None, List.empty, None))
      } //else context.become(checkAnalyser(readyCount + 1))
  }



  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case req: KillTask =>
      messageToAllReaders(req)
      sender ! JobKilled
      context.stop(self)
    case AreYouFinished => monitor = sender() // register to message out later
    case unhandled     => log.error(s"Not handled message in $description: " + unhandled)
  }

  private def messagePartitionManagers[T](msg: T): Unit =
    getAllPartitionManagers().foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messageToAllReaders[T](msg: T): Unit =
    getAllReaders().foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messagetoAllJobWorkers[T](msg:T):Unit =
    workerList.values.foreach(worker => worker ! msg)

  private def killJob() = {
    messagetoAllJobWorkers(KillTask(jobID))
    self ! PoisonPill
    if(monitor!=null)monitor ! TaskFinished(true)
  }

}

object QueryHandler {

  object Message{
    case object StartAnalysis

    case object ReaderWorkersOnline
    case object ReaderWorkersAck

    case class LoadAnalyser(jobId: String, className: String, args: List[String])
    case class EstablishExecutor(jobID:String)
    case class ExecutorEstablished(worker:Int, me:ActorRef)

    case object TimeCheck
    case class TimeResponse(time: Long)
    case object RecheckTime

    case class  CreatePerspective(neighbours: mutable.Map[Int,ActorRef], timestamp: Long, window: Option[Long])
    case object PerspectiveEstablished
    case class  StartSubtask(jobId: String)
    case class  Ready(messages: Int)
    case class  SetupNextStep(jobId: String)
    case object SetupNextStepDone
    case class  StartNextStep(jobId: String)
    case class  CheckMessages(jobId: String)
    case class  MessagesReceived(receivedMessages: Int, sentMessages: Int)
    case class  EndStep(superStep: Int, sentMessageCount: Int, voteToHalt: Boolean)
    case class  Finish(jobId: String)
    case class  ReturnResults(results: Any)
    case object StartNextSubtask
  }
}
