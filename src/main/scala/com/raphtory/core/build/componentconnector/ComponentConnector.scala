package com.raphtory.core.build.componentconnector

import akka.actor.{ActorRef, Cancellable, Props}
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.util.Timeout
import com.raphtory.core.components.actor.RaphtoryActor
import com.raphtory.core.components.partitionmanager.{PartitionManager, Writer}
import com.raphtory.core.components.analysismanager.AnalysisRestApi.message.{LiveAnalysisRequest, RangeAnalysisRequest, ViewAnalysisRequest}
import com.raphtory.core.components.raphtoryleader.WatchDog.Message.AssignedId
import com.raphtory.core.model.graph.GraphPartition

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps


abstract class ComponentConnector()
        extends RaphtoryActor {

  // TODO Make implicit timeouts as secondary (curried), optional implicit parameter
  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  var myId: Int                = -1
  var actorRef: ActorRef       = _
  var actorRefReader: ActorRef = _

  //mediator ! DistributedPubSubMediator.Subscribe(partitionsTopic, self)

  def callTheWatchDog(): Future[Any]

  def giveBirth(assignedId: Int): Unit

  override def preStart(): Unit = {
    log.debug("Replicator [{}] is being started.")

    scheduleTasks()
  }

  override def postStop(): Unit = {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        cancelTask(key, task)
    }

    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  def receive: Receive = {
    case msg: String if msg == "tick" => processHeartbeatMessage(msg)
    case _: SubscribeAck              =>
    case x                            => log.warning(s"Replicator received unknown [{}] message.", x)
  }

  def processHeartbeatMessage(msg: String): Unit = {
    log.debug(s"Replicator received [{}] message.", msg)

    if (myId == -1)
      try {
        val future = callTheWatchDog()
        myId = Await.result(future, timeout.duration).asInstanceOf[AssignedId].id

        giveBirth(myId)
      } catch {
        case _: java.util.concurrent.TimeoutException =>
          log.debug("Failed to retrieve Replicator Id due to timeout.")

          myId = -1

        case e: Exception => log.error("Failed to retrieve Replicator Id due to [{}].", e)
      }
  }



  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Replicator.")

    val tickCancellable =
      scheduleTask(initialDelay = 2 seconds, interval = 5 seconds, receiver = self, message = "tick")
    scheduledTaskMap.put("tick", tickCancellable)
  }
}


//case req: PartitionsCount         => processPartitionsCountRequest(req)

//  def processPartitionsCountRequest(req: PartitionsCount): Unit = {
//    log.debug(s"Replicator received [{}] request.", req)
//
//    if (req.count > totalPartitions) {
//      currentCount = req.count
//
//      if (actorRef != null)
//        actorRef ! UpdatedCounter(currentCount)
//      if (actorRefReader != null)
//        actorRef ! UpdatedCounter(currentCount)
//    }
//  }