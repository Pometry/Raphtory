package com.raphtory.core.actors.partitionmanager

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, OneForOneStrategy, Terminated}
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.RaphtoryActor.partitionsPerServer
import com.raphtory.core.actors.analysismanager.tasks.AnalysisTask.Message.{ReaderWorkersAck, ReaderWorkersOnline}
import com.raphtory.core.actors.orchestration.raphtoryleader.WatchDog.Message.PartitionUp
import com.raphtory.core.model.communication._
import com.raphtory.core.model.graph.GraphPartition

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class PartitionManager(
                     id: Int,
                     writers: ParTrieMap[Int, ActorRef],
                     readers: ParTrieMap[Int, ActorRef],
                     storage: ParTrieMap[Int, GraphPartition]
) extends RaphtoryActor {

  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()
  implicit val executionContext = context.system.dispatchers.lookup("misc-dispatcher")

  // Id which refers to the partitions position in the graph manager map
  val managerId: Int    = id
  val children: Int     = partitionsPerServer
  var lastLogTime: Long = System.currentTimeMillis() / 1000

  // should the handled messages be printed to terminal
  val printing: Boolean = false

  var messageCount: Int          = 0
  var secondaryMessageCount: Int = 0
  var workerMessageCount: Int    = 0

  val mediator: ActorRef = DistributedPubSub(context.system).mediator // get the mediator for sending cluster messages

  mediator ! DistributedPubSubMediator.Put(self)

  /**
    * Set up partition to report how many messages it has processed in the last X seconds
    */
  override def supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case e: Exception =>
      e.printStackTrace()
      Resume
  }

  override def preStart(): Unit = {
    log.debug("PartitionManager [{}] is being started.", managerId)

    scheduleTasks()
  }

  override def postStop(): Unit = {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        cancelTask(key, task)
    }

    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  override def receive: Receive = {
    case msg: String if msg == "count"      => processCountMessage(msg)
    case msg: String if msg == "keep_alive" => processKeepAliveMessage(msg)
    case ReaderWorkersOnline     => sender ! ReaderWorkersAck

    case Terminated(child) =>
      log.warning(s"Worker with path [{}] belonging to Manager [{}] has died.", child.path, managerId)
    case SubscribeAck              =>

    case x => log.warning(s"Partition Manager [{}] received unknown [{}] message.", managerId, x)
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

  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Writer [{}].", managerId)

    val countCancellable =
      scheduleTask(initialDelay = 10 seconds, interval = 1 seconds, receiver = self, message = "count")
    scheduledTaskMap.put("count", countCancellable)

    val keepAliveCancellable =
      scheduleTask(initialDelay = 10 seconds, interval = 10 seconds, receiver = self, message = "keep_alive")
    scheduledTaskMap.put("keep_alive", keepAliveCancellable)
  }
}
