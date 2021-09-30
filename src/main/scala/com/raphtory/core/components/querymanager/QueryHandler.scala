package com.raphtory.core.components.querymanager

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.RaphtoryActor.totalPartitions
import com.raphtory.core.components.analysismanager.tasks.AnalysisTask.SubtaskState
import com.raphtory.core.components.analysismanager.tasks.{SubTaskController, TaskTimeRange}
import com.raphtory.core.components.querymanager.QueryHandler.Message.{CheckMessages, CreatePerspective, EstablishExecutor, ExecutorEstablished, GraphFunctionComplete, PerspectiveEstablished, RecheckTime, SetupNextStep, StartAnalysis, StartGraph, StartSubtask, TableBuilt, TableFunctionComplete, TimeCheck, TimeResponse}
import com.raphtory.core.components.querymanager.QueryHandler.State
import com.raphtory.core.components.querymanager.QueryManager.Message.{AreYouFinished, JobFailed, JobKilled, KillTask, TaskFinished}
import com.raphtory.core.implementations.objectgraph.algorithm.{ObjectGraphPerspective, ObjectTable}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphFunction, Select, Table, TableFunction}

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

  ////OPERATION STATES
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

  private def executeGraph(state:State,readyCount:Int,sentMessageCount: Int, receivedMessageCount: Int): Receive = withDefaultMessageHandler("Execute Graph") {
    case StartGraph =>
      val graphPerspective = new ObjectGraphPerspective()
      algorithm.algorithm(graphPerspective)
      val table = graphPerspective.getTable()
      graphPerspective.getNextOperation() match {
        case Some(f:GraphFunction) =>
          messagetoAllJobWorkers(f)
          context.become(executeGraph(state.copy(graphPerspective=graphPerspective,table=table), 0,0,0))
        case None => killJob()
      }

    case GraphFunctionComplete(receivedMessages, sentMessages) =>
      if ( (readyCount+1) == totalPartitions)
        if ( (receivedMessageCount+receivedMessages) == (sentMessageCount+sentMessages) )
          nextGraphOperation(state)
        else {
          messagetoAllJobWorkers(CheckMessages(jobID))
          context.become(executeGraph(state, 0,0,0))
        }
      else
        context.become(executeGraph(state,(readyCount+1),(receivedMessageCount+receivedMessages),(sentMessageCount+sentMessages)))
  }

  private def executeTable(state:State,readyCount:Int): Receive = withDefaultMessageHandler("Execute Table") {
    case TableBuilt =>
      if ( (readyCount+1) == totalPartitions)
        nextTableOperation(state)
      else
        context.become(executeTable(state,(readyCount+1)))

    case TableFunctionComplete =>
      if ( (readyCount+1) == totalPartitions)
        nextTableOperation(state)
      else
        context.become(executeTable(state,(readyCount+1)))
  }
  ////END OPERATION STATES

  ///HELPER FUNCTIONS

  private def kickOffJob() = {
    val latestTime = whatsTheTime()
    val perspectiveController = buildPerspectiveController(latestTime)
    val currentPerspective = perspectiveController.nextPerspective()
    currentPerspective match {
      case Some(perspective) if perspective.timestamp <= latestTime =>
        log.info(s"$perspective for Job $jobID is ready to start")
        messagetoAllJobWorkers(CreatePerspective(workerList, perspective.timestamp, perspective.window))
        context.become(establishPerspective(State(perspectiveController, perspective, null,null),0))
      case Some(perspective) =>
        log.info(s"$perspective for Job $jobID is not ready, currently at $latestTime. Rechecking")
        context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, RecheckTime)
        context.become(establishPerspective(State(perspectiveController, perspective, null,null),0))
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

  private def nextGraphOperation(state:State) = {
    state.graphPerspective.getNextOperation() match {
      case Some(f:Select) =>
        messagetoAllJobWorkers(f)
        context.become(executeTable(state, 0))

      case Some(f: GraphFunction) =>
        messagetoAllJobWorkers(f)
        context.become(executeGraph(state, 0, 0, 0))

      case None =>
        killJob()
    }
  }

  private def nextTableOperation(state:State) = {
    state.table.getNextOperation() match {
      case Some(f: TableFunction) =>
        messagetoAllJobWorkers(f)
        context.become(executeTable(state, 0))

      case None =>
        killJob()
    }

  }

  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case req: KillTask => killJob()
    case AreYouFinished => monitor = sender() // register to message out later
    case unhandled     => log.error(s"Not handled message in $description: " + unhandled)
  }


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
  private case class State(perspectiveController: PerspectiveController, currentPerspective: Perspective, graphPerspective: ObjectGraphPerspective,table:ObjectTable) {
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
    case object TableBuilt
    case object TableFunctionComplete

    case class  CreatePerspective(neighbours: mutable.Map[Int,ActorRef], timestamp: Long, window: Option[Long])
    case object PerspectiveEstablished
    case class  StartSubtask(jobId: String)
    case class  Ready(messages: Int)
    case class  SetupNextStep(jobId: String)
    case object SetupNextStepDone
    case class  StartNextStep(jobId: String)
    case class  CheckMessages(jobId: String)
    case class  GraphFunctionComplete(receivedMessages: Int, sentMessages: Int)
    case class  EndStep(superStep: Int, sentMessageCount: Int, voteToHalt: Boolean)
    case class  Finish(jobId: String)
    case class  ReturnResults(results: Any)
    case object StartNextSubtask
  }
}
