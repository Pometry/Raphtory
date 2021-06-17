package com.raphtory.core.actors.clustermanagement

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.event.LoggingReceive
import com.raphtory.core.actors.clustermanagement.WatchDog.ActorState
import com.raphtory.core.actors.clustermanagement.WatchDog.Message._
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class WatchDog(managerCount: Int, minimumRouters: Int) extends RaphtoryActor {

  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val maxTimeInMillis = 30000

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit = {
    log.debug("WatchDog is being started.")
    context.system.scheduler.scheduleOnce(10.seconds, self, Tick)
    context.system.scheduler.scheduleOnce(3.minute, self, RefreshManagerCount)
  }

  override def receive: Receive =
    work(ActorState(clusterUp = false, pmLiveMap = Map.empty, roLiveMap = Map.empty, pmCounter = 0, roCounter = 0))

  private def work(state: ActorState): Receive = LoggingReceive {
    case Tick =>
      val newState = handleTick(state)
      context.become(work(newState))
      context.system.scheduler.scheduleOnce(10.seconds, self, Tick)

    case RefreshManagerCount =>
      mediator ! DistributedPubSubMediator.Publish(partitionsTopic, PartitionsCount(state.pmCounter))
      context.system.scheduler.scheduleOnce(1.minute, self, RefreshManagerCount)

    case ClusterStatusRequest =>
      sender ! ClusterStatusResponse(state.clusterUp, state.pmCounter, state.roCounter)

    case RequestPartitionCount =>
      log.debug(s"Sending Partition Manager count [${state.pmCounter}].")
      sender ! PartitionsCount(state.pmCounter)

    case PartitionUp(id) =>
      val newMap = state.pmLiveMap + (id -> System.currentTimeMillis())
      //println(s"Partition $id online")
      context.become(work(state.copy(pmLiveMap = newMap)))

    case RouterUp(id) =>
      val newMap = state.roLiveMap + (id -> System.currentTimeMillis())
      //println(s"Router $id online")
      context.become(work(state.copy(roLiveMap = newMap)))

    case RequestPartitionId =>
      sender() ! AssignedId(state.pmCounter)
      val newCounter = state.pmCounter + 1
      log.debug(s"Propagating the new total partition managers [$newCounter] to all the subscribers.")
      context.become(work(state.copy(pmCounter = newCounter)))
      mediator ! DistributedPubSubMediator.Publish(partitionsTopic, PartitionsCount(newCounter))

    case RequestRouterId =>
      sender ! AssignedId(state.roCounter)
      val newCounter = state.roCounter + 1
      context.become(work(state.copy(roCounter = newCounter)))

    case unhandled => log.error(s"WatchDog received unknown [$unhandled] message.")
  }

  private def handleTick(state: ActorState): ActorState = {
    checkMapTime(state.pmLiveMap, "Partition Manager")
    checkMapTime(state.roLiveMap, "Router")
    if (state.roLiveMap.size >= minimumRouters && state.pmLiveMap.size >= managerCount) {
      log.info("Partition managers and min. number of Routers have joined cluster.")
      log.debug(s"The cluster was started with [$managerCount] Partition Managers and [$minimumRouters] >= Routers.")
      state.copy(clusterUp = true)
    } else {
      println(s"Cluster Starting: ${state.roLiveMap.size}/$minimumRouters Routers and ${state.pmLiveMap.size}/$managerCount Partitions Live")
      state
    }
  }

  private def checkMapTime(map: Map[Int, Long], mapType: String): Unit =
    map.foreach {
      case (pmId, startTime) =>
        if (startTime + maxTimeInMillis <= System.currentTimeMillis())
          log.debug(s"$mapType [$pmId] not responding since [$startTime}].")
    }
}

object WatchDog {
  object Message {
    case object Tick
    case object RefreshManagerCount
    case class RouterUp(id: Int)
    case class PartitionUp(id: Int)
    case object ClusterStatusRequest
    case class ClusterStatusResponse(clusterUp: Boolean, pmCounter: Int, roCounter: Int)
    case class AssignedId(id: Int)
    case object RequestPartitionId
    case object RequestRouterId
    case object RequestPartitionCount
    case class PartitionsCount(count: Int)
  }
  private case class ActorState(
      clusterUp: Boolean,
      pmLiveMap: Map[Int, Long],
      roLiveMap: Map[Int, Long],
      pmCounter: Int,
      roCounter: Int
  )
}
