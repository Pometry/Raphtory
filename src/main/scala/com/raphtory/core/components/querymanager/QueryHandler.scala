package com.raphtory.core.components.querymanager

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.RaphtoryActor.totalPartitions
import com.raphtory.core.components.analysismanager.tasks.AnalysisTask.SubtaskState
import com.raphtory.core.components.analysismanager.tasks.{SubTaskController, TaskTimeRange}
import com.raphtory.core.components.querymanager.QueryHandler.Message.{CheckMessages, CreatePerspective, EstablishExecutor, ExecutorEstablished, MessagesReceived, PerspectiveEstablished, RecheckTime, SetupNextStep, StartAnalysis, StartGraph, StartSubtask, TimeCheck, TimeResponse}
import com.raphtory.core.components.querymanager.QueryHandler.State
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, JobFailed, JobKilled, KillTask, TaskFinished}
import com.raphtory.core.implementations.objectgraph.algorithm.ObjectGraphPerspective
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphFunction}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.reflect.ClassTag.Any
import scala.util.{Failure, Success}

abstract class QueryHandler(jobID:String,algorithm:GraphAlgorithm) extends RaphtoryActor{

  private val workerList = mutable.Map[Int,ActorRef]()

  private var monitor:ActorRef = _

  protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController
  override def preStart() = context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis)
  override def receive: Receive = spawnExecutors(0)

  private def spawnExecutors(readyCount: Int): Receive = withDefaultMessageHandler("spawn executors") {
    case StartAnalysis =>
      messageToAllReaders(EstablishExecutor(jobID))

    case ExecutorEstablished(workerID,actor) => //analyser confirmed to be present within workers, send setup request to workers
      workerList += ((workerID,actor))
      if (readyCount + 1 == totalPartitions)
        kickOffJob()
      else
        context.become(spawnExecutors(readyCount + 1))
  }

  private def establishPerspective(state:State,readyCount:Int): Receive = withDefaultMessageHandler("establish perspective") {
    case RecheckTime =>
      recheckTime(state.currentPerspective)
    case PerspectiveEstablished =>
      if (readyCount + 1 == totalPartitions) {
        self ! StartGraph
        context.become(executeGraph(state, 0,0,0))
      }
      else
        context.become(establishPerspective(state, readyCount + 1))
    case JobFailed => killJob()

  }

  private def executeGraph(state:State,readyCount:Int,sentMessageCount: Int, receivedMessageCount: Int): Receive = withDefaultMessageHandler("execute") {
    case StartGraph =>
      val graphPerspective = new ObjectGraphPerspective()
      algorithm.algorithm(graphPerspective)
      graphPerspective.getNextOperation() match {
        case Some(f:GraphFunction) =>
          messagetoAllJobWorkers(f)
          context.become(executeGraph(state.copy(graphPerspective=graphPerspective), 0,0,0))
        case None => killJob()
      }

    case MessagesReceived(receivedMessages, sentMessages) =>
      val newReadyCount           = readyCount + 1
      val newTotalReceivedMessage = receivedMessageCount + receivedMessages
      val newTotalSentMessage     = sentMessageCount + sentMessages
      if (newReadyCount == totalPartitions)
        if (newTotalReceivedMessage == newTotalSentMessage) {
          state.graphPerspective.getNextOperation() match {
            case Some(f: GraphFunction) =>
              messagetoAllJobWorkers(f)
              context.become(executeGraph(state, 0, 0, 0))
            case None =>
              killJob()
          }
          context.become(executeGraph(state, 0,0,0))
        } else {
          messagetoAllJobWorkers(CheckMessages(jobID))
          context.become(executeGraph(state, 0,0,0))
        }
      else
        context.become(executeGraph(state, newReadyCount, newTotalReceivedMessage, newTotalSentMessage))
  }


  private def kickOffJob() = {
    val latestTime = whatsTheTime()
    val perspectiveController = buildPerspectiveController(latestTime)
    val currentPerspective = perspectiveController.nextPerspective()
    currentPerspective match {
      case Some(perspective) if perspective.timestamp <= latestTime =>
        log.info(s"$perspective for Job $jobID is ready to start")
        messagetoAllJobWorkers(CreatePerspective(workerList, perspective.timestamp, perspective.window))
        context.become(establishPerspective(State(perspectiveController,null,perspective),0))
      case Some(perspective) =>
        log.info(s"$perspective for Job $jobID is not ready, currently at $latestTime. Rechecking")
        context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, RecheckTime)
        context.become(establishPerspective(State(perspectiveController,null,perspective),0))
      case None =>
        log.info(s"no more sub tasks for $jobID")
        killJob()
    }
  }


  private def recheckTime(perspective: Perspective):Unit = {
    val time = whatsTheTime()
    if (perspective.timestamp <= time)
      messagetoAllJobWorkers(CreatePerspective(workerList, perspective.timestamp, perspective.window))
    else {
      log.info(s"$perspective for Job $jobID is not ready, currently at $time. Rechecking")
      context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, RecheckTime)
    }
  }


  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case req: KillTask => killJob()
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
    log.info(s"$jobID has no more perspectives. Query Handler ending execution.")
    self ! PoisonPill
    if(monitor!=null)monitor ! TaskFinished(true)
  }

}

object QueryHandler {
  private case class State(perspectiveController: PerspectiveController, graphPerspective: ObjectGraphPerspective, currentPerspective: Perspective) {
    def updatePerspective(f: Perspective => Perspective): State = copy(currentPerspective = f(currentPerspective))
  }

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

    case object StartGraph

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
