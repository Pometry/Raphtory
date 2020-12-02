package com.raphtory.core.components.ClusterManagement

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.event.LoggingReceive
import com.raphtory.core.components.ClusterManagement.WatchDog.ActorState
import com.raphtory.core.components.ClusterManagement.WatchDog.Message.RefreshManagerCount
import com.raphtory.core.components.ClusterManagement.WatchDog.Message.Tick
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final case class WatchDog(managerCount: Int, minimumRouters: Int) extends Actor with ActorLogging {
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
      mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(state.pmCounter))
      context.system.scheduler.scheduleOnce(1.minute, self, RefreshManagerCount)

    case ClusterStatusRequest =>
      sender ! ClusterStatusResponse(state.clusterUp, state.pmCounter, state.roCounter)

    case RequestPartitionCount =>
      log.debug(s"Sending Partition Manager count [${state.pmCounter}].")
      sender ! PartitionsCountResponse(state.pmCounter)

    case PartitionUp(id) =>
      val newMap = state.pmLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(pmLiveMap = newMap)))

    case RouterUp(id) =>
      val newMap = state.roLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(roLiveMap = newMap)))

    case RequestPartitionId =>
      sender() ! AssignedId(state.pmCounter)
      val newCounter = state.pmCounter + 1
      log.debug(s"Propagating the new total partition managers [$newCounter] to all the subscribers.")
      context.become(work(state.copy(pmCounter = newCounter)))
      mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(newCounter))

    case RequestRouterId =>
      sender ! AssignedId(state.roCounter)
      val newCounter = state.roCounter + 1
      context.become(work(state.copy(roCounter = newCounter)))

    case unhandled => log.error(s"WatchDog received unknown [$unhandled] message.")
  }

  private def handleTick(state: ActorState): ActorState = {
    checkMapTime(state.pmLiveMap, "Partition Manager")
    checkMapTime(state.roLiveMap, "Router")
    if (!state.clusterUp && state.roLiveMap.size >= minimumRouters && state.roLiveMap.size >= managerCount) {
      log.info("Partition managers and min. number of Routers have joined cluster.")
      log.debug(s"The cluster was started with [$managerCount] Partition Managers and [$minimumRouters] >= Routers.")
      state.copy(clusterUp = true)
    } else state
  }

  private def checkMapTime(map: Map[Int, Long], mapType: String): Unit =
    map.foreach {
      case (pmId, startTime) =>
        if (startTime + maxTimeInMillis <= System.currentTimeMillis())
          log.debug(s"$mapType [$pmId] not responding since [${Utils.unixToTimeStamp(startTime)}].")
    }
}

object WatchDog {
  object Message {
    case object Tick
    case object RefreshManagerCount
  }
  private case class ActorState(
      clusterUp: Boolean,
      pmLiveMap: Map[Int, Long],
      roLiveMap: Map[Int, Long],
      pmCounter: Int,
      roCounter: Int
  )
}
