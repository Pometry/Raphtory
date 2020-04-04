package com.raphtory.core.components.PartitionManager

import akka.actor.SupervisorStrategy.Resume
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.Terminated
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.PartitionManager.Workers.WriterLogger
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.SchedulerUtil

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class Writer(
    id: Int,
    test: Boolean,
    managerCountVal: Int,
    workers: ParTrieMap[Int, ActorRef],
    storage: ParTrieMap[Int, EntityStorage]
) extends Actor
        with ActorLogging {

  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  // Id which refers to the partitions position in the graph manager map
  val managerId: Int    = id
  val children: Int     = 10
  var lastLogTime: Long = System.currentTimeMillis() / 1000

  // should the handled messages be printed to terminal
  val printing: Boolean = false

  // TODO Migrate actorOf logic to Object.props
  val logChild: ActorRef        = context.actorOf(Props[WriterLogger].withDispatcher("logging-dispatcher"), s"logger")
  val logChildForSize: ActorRef = context.actorOf(Props[WriterLogger].withDispatcher("logging-dispatcher"), s"logger2")

  var managerCount: Int          = managerCountVal
  var messageCount: Int          = 0
  var secondaryMessageCount: Int = 0
  var workerMessageCount: Int    = 0

  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  mediator ! DistributedPubSubMediator.Put(self)

  storage.foreach {
    case (_, entityStorage) =>
      entityStorage.apply(printing, managerCount, managerId, mediator)
  }

  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case e: Exception =>
      e.printStackTrace()
      Resume
  }

  override def preStart(): Unit = {
    log.debug("Writer [{}] is being started.", managerId)

    scheduleTasks()
  }

  override def postStop(): Unit = {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        SchedulerUtil.cancelTask(key, task)
    }

    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  override def receive: Receive = {
    case msg: String if msg == "log"        => processLogMessage(msg)
    case msg: String if msg == "count"      => processCountMessage(msg)
    case msg: String if msg == "keep_alive" => processKeepAliveMessage(msg)
    case req: UpdatedCounter                => handleUpdatedCounterRequest(req)
    case Terminated(child) =>
      log.warning(s"WriterWorker with patch [{}] belonging to Writer id [{}] has died.", child.path, managerId)
    case x => log.warning(s"Writer [{}] received unknown message [{}].", managerId, x)
  }

  def processLogMessage(req: String): Unit = {
    log.debug(s"Writer [{}] received [{}] message.", managerId, req)

    logChildForSize ! ReportSize(managerId)
  }

  def processCountMessage(msg: String): Unit = {
    log.debug(s"Writer [{}] received [{}] message.", managerId, msg)

    val newTime        = System.currentTimeMillis() / 1000
    var timeDifference = newTime - lastLogTime
    if (timeDifference == 0) timeDifference = 1
  }

  def processKeepAliveMessage(msg: String): Unit = {
    log.debug(s"Writer [{}] received [{}] message.", managerId, msg)

    val sendMessage = PartitionUp(managerId)
    val sendPath    = "/user/WatchDog"
    mediator ! DistributedPubSubMediator.Send(sendPath, sendMessage, localAffinity = false)

    log.debug(s"DistributedPubSubMediator sent message [{}] to path [{}].", sendMessage, sendPath)
  }

  def handleUpdatedCounterRequest(req: UpdatedCounter): Unit = {
    log.debug(s"Writer [{}] received request [{}]", managerId, req)

    managerCount = req.newValue

    if (storage.isEmpty)
      log.warning("Entity storage is empty. The request [{}] will not be acted upon.", req)
    else
      storage.foreach {
        case (_, entityStorage) =>
          log.debug("Setting manager count for [{}] to [{}]", entityStorage, managerCount)

          entityStorage.setManagerCount(managerCount)
      }
  }

  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Writer [{}].", managerId)

    val logCancellable =
      SchedulerUtil.scheduleTask(initialDelay = 10 seconds, interval = 10 seconds, receiver = self, message = "log")
    scheduledTaskMap.put("log", logCancellable)

    val countCancellable =
      SchedulerUtil.scheduleTask(initialDelay = 10 seconds, interval = 1 seconds, receiver = self, message = "count")
    scheduledTaskMap.put("count", countCancellable)

    val keepAliveCancellable =
      SchedulerUtil
        .scheduleTask(initialDelay = 10 seconds, interval = 10 seconds, receiver = self, message = "keep_alive")
    scheduledTaskMap.put("keep_alive", keepAliveCancellable)
  }
}
