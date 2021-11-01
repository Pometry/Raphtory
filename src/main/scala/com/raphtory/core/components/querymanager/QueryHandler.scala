package com.raphtory.core.components.querymanager

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.querymanager.QueryHandler.Message._
import com.raphtory.core.components.querymanager.QueryHandler.State
import com.raphtory.core.components.querymanager.QueryManager.Message._
import com.raphtory.core.implementations.generic.algorithm.{ObjectGraphPerspective, GenericTable}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphFunction, Iterate, Select, Table, TableFunction}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, MILLISECONDS, SECONDS}
import scala.reflect.ClassTag.Any
import scala.util.{Failure, Success}

abstract class QueryHandler(jobID:String,algorithm: GraphAlgorithm) extends RaphtoryActor{

  private val workerList = mutable.Map[Int,ActorRef]()

  private var monitor:ActorRef = _

  protected def buildPerspectiveController(latestTimestamp: Long): PerspectiveController
  override def preStart() = context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis)
  override def receive: Receive = spawnExecutors(0)

  private var currentPerspective = Perspective(-1,Some(-1))
  private var lasttime = 0L

  ////OPERATION STATES
  //Communicate with all readers and get them to spawn a QueryExecutor for their partition
  private def spawnExecutors(readyCount: Int): Receive = withDefaultMessageHandler("spawn executors") {
    case StartAnalysis =>
      messageToAllReaders(EstablishExecutor(jobID))

    case ExecutorEstablished(workerID,actor) =>
      workerList += ((workerID,actor))
      if (readyCount + 1 == totalPartitions) {
        val latestTime = whatsTheTime()
        val perspectiveController = buildPerspectiveController(latestTime)
        executeNextPerspective(perspectiveController)
      } else
        context.become(spawnExecutors(readyCount + 1))
  }

  //build the perspective within the QueryExecutor for each partition -- the view to be analysed
  private def establishPerspective(state:State,readyCount:Int,vertexCount:Int): Receive = withDefaultMessageHandler("establish perspective") {
    case RecheckTime =>
      recheckTime(state.currentPerspective)
    case p:PerspectiveEstablished =>
      if (readyCount + 1 == totalPartitions) {
        messagetoAllJobWorkers(SetMetaData(vertexCount+p.vertices))
        context.become(establishPerspective(state, 0,vertexCount+p.vertices))
      }
      else
        context.become(establishPerspective(state, readyCount + 1,vertexCount+p.vertices))

    case MetaDataSet =>
      if (readyCount + 1 == totalPartitions) {
        self ! StartGraph
        context.become(executeGraph(state, null, vertexCount, 0, 0, 0, true))
      }
      else
        context.become(establishPerspective(state, readyCount + 1,vertexCount))
  }

  //execute the steps of the graph algorithm until a select is run
  private def executeGraph(state: State, currentOpperation: GraphFunction, vertexCount:Int, readyCount: Int, receivedMessageCount: Int, sentMessageCount: Int, allVoteToHalt: Boolean):Receive = withDefaultMessageHandler("Execute Graph") {
    case StartGraph =>
      val graphPerspective = new ObjectGraphPerspective(vertexCount)
      algorithm.algorithm(graphPerspective)
      val table = graphPerspective.getTable()
      graphPerspective.getNextOperation() match {
        case Some(f:Select) =>
          messagetoAllJobWorkers(f)
          context.become(executeTable(state.copy(graphPerspective=graphPerspective,table=table), 0))

        case Some(f:GraphFunction) =>
          messagetoAllJobWorkers(f)
          context.become(executeGraph(state.copy(graphPerspective=graphPerspective,table=table), f, vertexCount,0, 0, 0, true))
        case None => killJob()
      }

    case GraphFunctionComplete(receivedMessages, sentMessages,votedToHalt) =>
      val totalSentMessages = sentMessageCount+sentMessages
      val totalReceivedMessages = (receivedMessageCount+receivedMessages)
      if ( (readyCount+1) == totalPartitions) {
        if ( (receivedMessageCount+receivedMessages) == (sentMessageCount+sentMessages) ) {
          currentOpperation match {
            case Iterate(f, iterations,executeMessagedOnly) =>
              if(iterations==1||(allVoteToHalt&&votedToHalt)) {
                nextGraphOperation(state,vertexCount)
              } else  {
                messagetoAllJobWorkers(Iterate(f, iterations-1,executeMessagedOnly))
                context.become(executeGraph(state, Iterate(f, iterations-1,executeMessagedOnly), vertexCount,0, 0, 0, true))
              }
            case _ =>
              nextGraphOperation(state,vertexCount)
          }
        }
        else {
          messagetoAllJobWorkers(CheckMessages(jobID))
          context.become(executeGraph(state, currentOpperation,vertexCount, 0, 0, 0, allVoteToHalt))
        }
      }
      else
        context.become(executeGraph(state, currentOpperation, vertexCount, (readyCount+1), totalReceivedMessages, totalSentMessages, (votedToHalt&allVoteToHalt)))
  }

  //once the select has been run, execute all of the table functions until we hit a writeTo
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
  private def executeNextPerspective(perspectiveController: PerspectiveController) = {
    val latestTime = whatsTheTime()
    val currentPerspective = perspectiveController.nextPerspective()

    currentPerspective match {
      case Some(perspective) if perspective.timestamp <= latestTime =>
        //log.info(s"$perspective for Job $jobID is starting")
        logTime(perspective)
        messagetoAllJobWorkers(CreatePerspective(workerList, perspective.timestamp, perspective.window))
        context.become(establishPerspective(State(perspectiveController, perspective, null,null),0,0))
      case Some(perspective) =>
        log.info(s"$perspective for Job $jobID is not ready, currently at $latestTime. Rechecking")
        logTime(perspective)
        context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, RecheckTime)
        context.become(establishPerspective(State(perspectiveController, perspective, null,null),0,0))
      case None =>
        log.info(s"no more perspectives to run for $jobID")
        killJob()
    }
  }

  private def logTime(perspective: Perspective) = {
    if(currentPerspective.timestamp!=(-1)){

      currentPerspective.window match {
        case Some(window) =>
          log.info(s"For $jobID - Perspective at Time ${currentPerspective.timestamp} with Window $window took ${System.currentTimeMillis()-lasttime} milliseconds to run.")
        case None =>
          log.info(s"For $jobID - Perspective at Time ${currentPerspective.timestamp} took ${System.currentTimeMillis()-lasttime} milliseconds to run. ")
      }
      lasttime = System.currentTimeMillis()
      currentPerspective = perspective

    }
    else{
      currentPerspective = perspective
      lasttime = System.currentTimeMillis()
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

  private def nextGraphOperation(state:State,vertexCount:Int) = {
    state.graphPerspective.getNextOperation() match {
      case Some(f:Select) =>
        messagetoAllJobWorkers(f)
        context.become(executeTable(state, 0))

      case Some(f: GraphFunction) =>
        messagetoAllJobWorkers(f)
        context.become(executeGraph(state, f,vertexCount, 0, 0, 0, true))

      case None =>
        executeNextPerspective(state.perspectiveController)
    }
  }

  private def nextTableOperation(state:State) = {
    state.table.getNextOperation() match {
      case Some(f: TableFunction) =>
        messagetoAllJobWorkers(f)
        context.become(executeTable(state, 0))

      case None =>
        executeNextPerspective(state.perspectiveController)
    }

  }

  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case req: EndQuery => killJob()
    case AreYouFinished => monitor = sender() // register to message out later
    case unhandled     => log.error(s"Not handled message in $description: " + unhandled)
  }


  private def messageToAllReaders[T](msg: T): Unit =
    getAllReaders().foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messagetoAllJobWorkers[T](msg:T):Unit =
    workerList.values.foreach(worker => worker ! msg)

  private def killJob() = {
    messagetoAllJobWorkers(EndQuery(jobID))
    log.info(s"$jobID has no more perspectives. Query Handler ending execution.")
    self ! PoisonPill
    if(monitor!=null)monitor ! TaskFinished(true)
  }

}

object QueryHandler {
  private case class State(perspectiveController: PerspectiveController, currentPerspective: Perspective, graphPerspective: ObjectGraphPerspective,table:GenericTable) {
    def updatePerspective(f: Perspective => Perspective): State = copy(currentPerspective = f(currentPerspective))
  }

  object Message{
    case object StartAnalysis
    case class EstablishExecutor(jobID:String)
    case class ExecutorEstablished(worker:Int, me:ActorRef)

    case class  CreatePerspective(neighbours: mutable.Map[Int,ActorRef], timestamp: Long, window: Option[Long])
    case class  PerspectiveEstablished(vertices:Int)
    case class  SetMetaData(vertices:Int)
    case object MetaDataSet

    case object StartGraph
    case class  GraphFunctionComplete(receivedMessages: Int, sentMessages: Int,votedToHalt:Boolean=false)

    case object TableBuilt
    case object TableFunctionComplete

    case object RecheckTime
    case class  CheckMessages(jobId: String)
  }
}
