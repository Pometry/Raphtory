package com.raphtory.core.actors.AnalysisManager.Tasks

import akka.actor.PoisonPill
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.Message
import com.raphtory.core.actors.AnalysisManager.AnalysisManager.Message._
import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask.Message._
import com.raphtory.core.actors.AnalysisManager.Tasks.AnalysisTask.SubtaskState
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.analysis.api.{AggregateSerialiser, Analyser}
import kamon.Kamon

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

abstract class AnalysisTask(
    jobId: String,
    args: Array[String],
    analyser: Analyser[Any],
    serialiser:AggregateSerialiser,
    managerCount: Int,
    newAnalyser: Boolean,
    rawFile: String
) extends RaphtoryActor {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  //mediator ! DistributedPubSubMediator.Subscribe(partitionsTopic, self)

  private val maxStep: Int     = analyser.defineMaxSteps()
  private val workerCount: Int = managerCount * totalWorkers

  protected def buildSubTaskController(readyTimestamp: Long): SubTaskController

  override def preStart() {
    context.system.scheduler.scheduleOnce(Duration(1, MILLISECONDS), self, StartAnalysis)
  }

  override def receive: Receive = checkReaderWorker(0)

  private def withDefaultMessageHandler(description: String)(handler: Receive): Receive = handler.orElse {
    case RequestResults(_) =>
      sender ! ResultsForApiPI(Array())//TODO remove
    case req: KillTask =>
      messageToAllReaderWorkers(req)
      sender ! JobKilled
      context.stop(self)
    case unhandled     => log.error(s"Not handled message in $description: " + unhandled)
  }

  private def checkReaderWorker(readyCount: Int): Receive = withDefaultMessageHandler("check reader worker") {
    case StartAnalysis => //received message to start from Orchestrator
      messageToAllReaders(ReaderWorkersOnline)

    case ReaderWorkersAck => //count up number of acks and if == number of workers, check if analyser present
      if (readyCount + 1 != managerCount)
        context.become(checkReaderWorker(readyCount + 1))
      else if (newAnalyser) {
        messageToAllReaderWorkers(CompileNewAnalyser(jobId, rawFile, args.toList))
        context.become(checkAnalyser(0))
      } else {
        messageToAllReaderWorkers(LoadPredefinedAnalyser(jobId, analyser.getClass.getCanonicalName, args.toList))
        context.become(checkAnalyser(0))
      }
  }

  private def checkAnalyser(readyCount: Int): Receive = withDefaultMessageHandler("check analyser") {
    case FailedToCompile(stackTrace) => //Your code is broke scrub
      log.info(s"$sender failed to compiled, stacktrace returned: \n $stackTrace")

    case AnalyserPresent => //analyser confirmed to be present within workers, send setup request to workers
      if (readyCount + 1 == workerCount) {
        messageToAllReaderWorkers(TimeCheck)
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
      if (readyTimes.size == workerCount) {
        val controller = taskController.getOrElse(buildSubTaskController(time))
        val readyTime  = readyTimes.min
        currentRange.orElse(controller.nextRange(readyTime)) match {
          case Some(range) if range.timestamp <= readyTime =>
            log.info(s"Range $range for Job $jobId is ready to start")
            println("hello")
            messagetoAllJobWorkers(SetupSubtask(jobId, range.timestamp, range.window))
            //messageToAllReaderWorkers(SetupSubtask(jobId, range.timestamp, range.window))
            context.become(waitAllReadyForSetupTask(SubtaskState(range, System.currentTimeMillis(), controller), 0))
          case Some(range) =>
            log.info(s"Range $range for Job $jobId is not ready. Recheck")
            context.system.scheduler.scheduleOnce(Duration(10, SECONDS), self, RecheckTime)
            context.become(checkTime(Some(controller), List.empty, Some(range)))
          case None =>
            log.info(s"no more sub tasks for $jobId")
            messagetoAllJobWorkers(KillTask(jobId))
            self ! PoisonPill
        }
      } else
        context.become(checkTime(taskController, readyTimes, currentRange))
    case RecheckTime => //if the time was previous out of scope, wait and then recheck
      messageToAllReaderWorkers(TimeCheck)
  }

  private def waitAllReadyForSetupTask(subtaskState: SubtaskState, readyCount: Int): Receive =
    withDefaultMessageHandler("ready for setup task") {
      case SetupSubtaskDone =>
        val newReadyCount = readyCount + 1
        println(s"$newReadyCount")
        if (newReadyCount == workerCount) {
          messagetoAllJobWorkers(StartSubtask(jobId))
          context.become(preStepSubtask(subtaskState, 0, 0))
        }
        else context.become(waitAllReadyForSetupTask(subtaskState, newReadyCount))
    }

  private def preStepSubtask(subtaskState: SubtaskState, readyCount: Int, sentMessageCount: Int): Receive =
    withDefaultMessageHandler("pre-setup subtask") {
      case Ready(messagesSent) =>
        val newReadyCount       = readyCount + 1
        val newSendMessageCount = sentMessageCount + messagesSent
        log.debug(s"setup workers $newReadyCount / $workerCount")
        if (newReadyCount == workerCount)
          if (newSendMessageCount == 0) {
            messagetoAllJobWorkers(SetupNextStep(jobId))
            context.become(waitAllReadyForNextStep(subtaskState, 0))
          } else {
            messagetoAllJobWorkers(CheckMessages(jobId))
            context.become(checkMessages(subtaskState, 0, 0, 0))
          }
        else context.become(preStepSubtask(subtaskState, newReadyCount, newSendMessageCount))
    }

  private def waitAllReadyForNextStep(subtaskState: SubtaskState, readyCount: Int): Receive =
    withDefaultMessageHandler("ready for next step") {
      case SetupNextStepDone =>
        val newReadyCount = readyCount + 1
        if (newReadyCount == workerCount) {
          messagetoAllJobWorkers(StartNextStep(jobId))
          context.become(stepWork(subtaskState, 0, 0, true))
        }
        else context.become(waitAllReadyForNextStep(subtaskState, newReadyCount))
    }

  private def checkMessages(
      subtaskState: SubtaskState,
      readyCount: Int,
      totalReceivedMessage: Int,
      totalSentMessage: Int
  ): Receive = withDefaultMessageHandler("check message") {
    case MessagesReceived(receivedMessages, sentMessages) =>
      val newReadyCount           = readyCount + 1
      val newTotalReceivedMessage = totalReceivedMessage + receivedMessages
      val newTotalSentMessage     = totalSentMessage + sentMessages
      if (newReadyCount == workerCount)
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
      if (newReadyCount == workerCount)
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
  }

  private def finishSubtask(subtaskState: SubtaskState, readyCount: Int, allResults: List[Any]): Receive =
    withDefaultMessageHandler("finish subtask") {
      case ReturnResults(results) =>
        val newReadyCount = readyCount + 1
        val newAllResults = allResults :+ results
        if (newReadyCount == workerCount) {
          val startTime = System.currentTimeMillis()
          val viewTime = Kamon
            .gauge("Raphtory_View_Time_Total")
            .withTag("jobID", jobId)
            .withTag("Timestamp", subtaskState.range.timestamp)
          val concatTime = Kamon
            .gauge("Raphtory_View_Concatenation_Time")
            .withTag("jobID", jobId)
            .withTag("Timestamp", subtaskState.range.timestamp)
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
          concatTime.update(System.currentTimeMillis() - startTime)
          viewTime.update(System.currentTimeMillis() - subtaskState.startTimestamp)
        } else
          context.become(finishSubtask(subtaskState, newReadyCount, newAllResults))

      case StartNextSubtask =>
        messageToAllReaderWorkers(TimeCheck)
        context.become(checkTime(Some(subtaskState.taskController), List.empty, None))
    }

  private def messageToAllReaders[T](msg: T): Unit =
    getAllReaders(managerCount).foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messageToAllReaderWorkers[T](msg: T): Unit =
    getAllReaderWorkers(managerCount).foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))

  private def messagetoAllJobWorkers[T](msg:T):Unit =
    getAllJobWorkers(managerCount,jobId).foreach(worker => mediator ! new DistributedPubSubMediator.Send(worker, msg))



}

object AnalysisTask {
  private case class SubtaskState(range: TaskTimeRange, startTimestamp: Long, taskController: SubTaskController)

  object Message {
    case object StartAnalysis

    case object ReaderWorkersOnline
    case object ReaderWorkersAck

    case class CompileNewAnalyser(jobId: String, analyserRaw: String, args: List[String])
    case class LoadPredefinedAnalyser(jobId: String, className: String, args: List[String])
    case class FailedToCompile(stackTrace: String)
    case object AnalyserPresent

    case object TimeCheck
    case class TimeResponse(time: Long)
    case object RecheckTime

    case class SetupSubtask(jobId: String, timestamp: Long, window: Option[Long])
    case object ReadyToRock
    case object SetupSubtaskDone
    case class StartSubtask(jobId: String)
    case class Ready(messages: Int)
    case class SetupNextStep(jobId: String)
    case object SetupNextStepDone
    case class StartNextStep(jobId: String)
    case class CheckMessages(jobId: String)
    case class MessagesReceived(receivedMessages: Int, sentMessages: Int)
    case class EndStep(superStep: Int, sentMessageCount: Int, voteToHalt: Boolean)
    case class Finish(jobId: String)
    case class ReturnResults(results: Any)
    case object StartNextSubtask
  }
}
