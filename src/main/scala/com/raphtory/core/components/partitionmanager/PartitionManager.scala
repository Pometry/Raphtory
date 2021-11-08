package com.raphtory.core.components.partitionmanager

import akka.actor.SupervisorStrategy.Resume
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, OneForOneStrategy, Terminated}
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.akkamanagement.{MailboxTrackingExtension, MailboxTrackingExtensionImpl, RaphtoryActor}
import com.raphtory.core.components.graphbuilder.BuilderExecutor.Message.{DataFinishedSync, PartitionRequest}
import com.raphtory.core.components.leader.WatchDog.Message.PartitionUp
import com.raphtory.core.implementations.generic.messaging._
import com.raphtory.core.model.graph.GraphPartition

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */
class PartitionManager(
                     id: Int,
                     writers: mutable.Map[Int, ActorRef],
                     readers: mutable.Map[Int, ActorRef],
                     storage: mutable.Map[Int, GraphPartition]
) extends RaphtoryActor {

  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  // Id which refers to the partitions position in the graph manager map
  val managerId: Int    = id
  val children: Int     = partitionsPerServer


  val mailBoxCounter: MailboxTrackingExtensionImpl = MailboxTrackingExtension(context.system)

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
    case msg: String if msg == "pull"      => processCountMessage(msg)
    case msg: String if msg == "keep_alive" => processKeepAliveMessage(msg)

    case Terminated(child) =>
      log.warning(s"Worker with path [{}] belonging to Manager [{}] has died.", child.path, managerId)
    case SubscribeAck              =>

    case x => log.warning(s"Partition Manager [{}] received unknown [{}] message.", managerId, x)
  }

  def processCountMessage(msg: String): Unit = {
    writers.foreach{
      case (id,writer) =>
        if(mailBoxCounter.current(writer.path) < RaphtoryActor.partitionMinQueue)
          getAllGraphBuilders().foreach { workerPath =>
            mediator ! new DistributedPubSubMediator.Send(
              workerPath,
              PartitionRequest(id)
            )
          }
    }
  }


//  def requestData() = {
//
//    val workerPath = getAllGraphBuilders()(pullCount%RaphtoryActor.totalBuilders)
//    mediator ! new DistributedPubSubMediator.Send(workerPath,PartitionRequest(partitionID))
//    pullCount+=1
//
//    if(updatesBefore<updates)
//      self! RequestData
//    else
//      scheduleTaskOnce(1 seconds, receiver = self, message = RequestData)
//    updatesBefore = updates
//  }

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
      scheduleTask(initialDelay = 1 seconds, interval = 100 millisecond, receiver = self, message = "pull")
    scheduledTaskMap.put("pull", countCancellable)

    val keepAliveCancellable =
      scheduleTask(initialDelay = 10 seconds, interval = 10 seconds, receiver = self, message = "keep_alive")
    scheduledTaskMap.put("keep_alive", keepAliveCancellable)
  }
}
