package com.raphtory.core.components.analysismanager.tasks

import akka.actor.{ActorRef, PoisonPill}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.querymanager.QueryManager.Message._
import com.raphtory.core.components.querymanager.QueryHandler.Message._
import com.raphtory.core.components.analysismanager.tasks.AnalysisTask.SubtaskState
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.model.algorithm.{AggregateSerialiser, Analyser}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

abstract class AnalysisTask(jobId: String, args: Array[String], analyser: Analyser[Any], serialiser:AggregateSerialiser) extends RaphtoryActor {
  private val workerList = mutable.Map[Int,ActorRef]()
  private val maxStep: Int     = analyser.defineMaxSteps()

  private var monitor:ActorRef = _

  protected def buildSubTaskController(readyTimestamp: Long): SubTaskController

  override def preStart() {
    context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis)
  }

  override def receive: Receive = checkReaderWorker(0)

  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case RequestResults(_) =>
      sender ! ResultsForApiPI(Array())//TODO remove
    case req: KillTask =>
      messageToAllReaders(req)
      sender ! JobKilled
      context.stop(self)
    case AreYouFinished => monitor = sender() // register to message out later
    case unhandled     => log.error(s"Not handled message in $description: " + unhandled)
  }

  private def checkReaderWorker(readyCount: Int): Receive = withDefaultMessageHandler("check reader worker") {
    case StartAnalysis => //received message to start from Orchestrator
      messagePartitionManagers(ReaderWorkersOnline)

    case ReaderWorkersAck => //count up number of acks and if == number of workers, check if analyser present
      if (readyCount + 1 != partitionServers) {
        context.become(checkReaderWorker(readyCount + 1))
      }
      else {
        println("checking analyser")
        messageToAllReaders(LoadAnalyser(jobId, analyser.getClass.getCanonicalName, args.toList))
        context.become(checkAnalyser(0))

      }
  }

  private def checkAnalyser(readyCount: Int): Receive = withDefaultMessageHandler("check analyser") {
    case ExecutorEstablished(workerID,actor) => //analyser confirmed to be present within workers, send setup request to workers
      workerList += ((workerID,actor))
      if (readyCount + 1 == totalPartitions) {
        messageToAllReaders(TimeCheck)
        context.become(checkTime(None, List.empty, None))
      } else context.become(checkAnalyser(readyCount + 1))
  }

  private def checkTime(
      taskController: Option[SubTaskController],
      readyTimeList: List[Long],
      currentRange: Option[TaskTimeRange]
  ): Receive = withDefaultMessageHandler("check time") {
    case TimeResponse(time) =>
      val readyTimes = time :: readyTimeList
      if (readyTimes.size == totalPartitions) {
        val controller = taskController.getOrElse(buildSubTaskController(time))
        val readyTime  = readyTimes.min
        currentRange.orElse(controller.nextRange(readyTime)) match {
          case Some(range) if range.timestamp <= readyTime =>
            log.info(s"Range $range for Job $jobId is ready to start")
            messagetoAllJobWorkers(CreatePerspective(workerList, range.timestamp, range.window))
            context.become(waitAllReadyForSetupTask(SubtaskState(range, System.currentTimeMillis(), controller), 0))
          case Some(range) =>
            log.info(s"Range $range for Job $jobId is not ready. Recheck")
            context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, RecheckTime)
            context.become(checkTime(Some(controller), List.empty, Some(range)))
          case None =>
            log.info(s"no more sub tasks for $jobId")
            killJob()
        }
      } else
        context.become(checkTime(taskController, readyTimes, currentRange))
    case RecheckTime => //if the time was previous out of scope, wait and then recheck
      messageToAllReaders(TimeCheck)
  }

  private def waitAllReadyForSetupTask(subtaskState: SubtaskState, readyCount: Int): Receive =
    withDefaultMessageHandler("ready for setup task") {
      case PerspectiveEstablished =>
        val newReadyCount = readyCount + 1
        if (newReadyCount == totalPartitions) {
          messagetoAllJobWorkers(StartSubtask(jobId))
          context.become(preStepSubtask(subtaskState, 0, 0))
        }
        else context.become(waitAllReadyForSetupTask(subtaskState, newReadyCount))
      case JobFailed => killJob()
    }

  private def preStepSubtask(subtaskState: SubtaskState, readyCount: Int, sentMessageCount: Int): Receive =
    withDefaultMessageHandler("pre-setup subtask") {
      case Ready(messagesSent) =>
        val newReadyCount       = readyCount + 1
        val newSendMessageCount = sentMessageCount + messagesSent
        log.debug(s"setup workers $newReadyCount / $totalPartitions")
        if (newReadyCount == totalPartitions)
          if (newSendMessageCount == 0) {
            messagetoAllJobWorkers(SetupNextStep(jobId))
            context.become(waitAllReadyForNextStep(subtaskState, 0))
          } else {
            messagetoAllJobWorkers(CheckMessages(jobId))
            context.become(checkMessages(subtaskState, 0, 0, 0))
          }
        else context.become(preStepSubtask(subtaskState, newReadyCount, newSendMessageCount))

      case JobFailed => killJob()
    }

  private def waitAllReadyForNextStep(subtaskState: SubtaskState, readyCount: Int): Receive =
    withDefaultMessageHandler("ready for next step") {
      case SetupNextStepDone =>
        val newReadyCount = readyCount + 1
        if (newReadyCount == totalPartitions) {
          messagetoAllJobWorkers(StartNextStep(jobId))
          context.become(stepWork(subtaskState, 0, 0, true))
        }
        else context.become(waitAllReadyForNextStep(subtaskState, newReadyCount))

      case JobFailed => killJob()
    }

  private def checkMessages(
      subtaskState: SubtaskState,
      readyCount: Int,
      totalReceivedMessage: Int,
      totalSentMessage: Int
  ): Receive = withDefaultMessageHandler("check message") {
    case GraphFunctionComplete(receivedMessages, sentMessages,votedToHalt) =>
      val newReadyCount           = readyCount + 1
      val newTotalReceivedMessage = totalReceivedMessage + receivedMessages
      val newTotalSentMessage     = totalSentMessage + sentMessages
      if (newReadyCount == totalPartitions)
        if (newTotalReceivedMessage == newTotalSentMessage) {
          messagetoAllJobWorkers(SetupNextStep(jobId))
          context.become(waitAllReadyForNextStep(subtaskState, 0))
        } else {
          messagetoAllJobWorkers(CheckMessages(jobId))
          context.become(checkMessages(subtaskState, 0, 0, 0))
        }
      else
        context.become(checkMessages(subtaskState, newReadyCount, newTotalReceivedMessage, newTotalSentMessage))
  }

  private def stepWork(
      subtaskState: SubtaskState,
      readyCount: Int,
      totalSentMessages: Int,
      voteToHaltForAll: Boolean
  ): Receive = withDefaultMessageHandler("step work") {
    case EndStep(superStep, sentMessageCount, voteToHalt) =>
      val newReadyCount        = readyCount + 1
      val newTotalSentMessages = totalSentMessages + sentMessageCount
      val newVoteToHaltForAll  = if (voteToHalt) voteToHaltForAll else false
      if (newReadyCount == totalPartitions)
        if (superStep == maxStep || newVoteToHaltForAll) {
          messagetoAllJobWorkers(Finish(jobId))
          context.become(finishSubtask(subtaskState, 0, List.empty))
        } else if (newTotalSentMessages == 0) {
          messagetoAllJobWorkers(SetupNextStep(jobId))
          context.become(waitAllReadyForNextStep(subtaskState, 0))
        } else {
          messagetoAllJobWorkers(CheckMessages(jobId))
          context.become(checkMessages(subtaskState, 0, 0, 0))
        }
      else
        context.become(stepWork(subtaskState, newReadyCount, newTotalSentMessages, newVoteToHaltForAll))

    case JobFailed => killJob()
  }

  private def finishSubtask(subtaskState: SubtaskState, readyCount: Int, allResults: List[Any]): Receive =
    withDefaultMessageHandler("finish subtask") {
      case ReturnResults(results) =>
        val newReadyCount = readyCount + 1
        val newAllResults = allResults :+ results
        if (newReadyCount == totalPartitions) {
          Try {
            val viewTime = System.currentTimeMillis() - subtaskState.startTimestamp
            subtaskState.range.window match {
              case Some(window) => serialiser.serialiseWindowedView(analyser.extractResults(newAllResults),subtaskState.range.timestamp,window,jobId,viewTime)
              case None => serialiser.serialiseView(analyser.extractResults(newAllResults),subtaskState.range.timestamp,jobId,viewTime)
            }
          } match {
            case Success(_) =>
              context.system.scheduler
                .scheduleOnce(Duration(subtaskState.taskController.waitTime, MILLISECONDS), self, StartNextSubtask)
            case Failure(e) =>
              log.error(e, "fail to process result")
          }
        } else
          context.become(finishSubtask(subtaskState, newReadyCount, newAllResults))

      case StartNextSubtask =>
        messageToAllReaders(TimeCheck)
        context.become(checkTime(Some(subtaskState.taskController), List.empty, None))

      case JobFailed => killJob()
    }

  private def messagePartitionManagers[T](msg: T): Unit =
    getAllPartitionManagers().foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messageToAllReaders[T](msg: T): Unit =
    getAllReaders().foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messagetoAllJobWorkers[T](msg:T):Unit =
    workerList.values.foreach(worker => worker ! msg)

  private def killJob() = {
    messagetoAllJobWorkers(KillTask(jobId))
    self ! PoisonPill
    if(monitor!=null)monitor ! TaskFinished(true)
  }

}

object AnalysisTask {
  private case class SubtaskState(range: TaskTimeRange, startTimestamp: Long, taskController: SubTaskController)

}
