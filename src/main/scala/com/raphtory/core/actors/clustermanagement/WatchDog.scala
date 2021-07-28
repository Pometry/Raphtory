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

class WatchDog(managerCount: Int, minimumRouters: Int,spoutCount:Int=1,analysisCount:Int=1) extends RaphtoryActor {


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
    work(ActorState(clusterUp = false, pmLiveMap = Map.empty, roLiveMap = Map.empty,spLiveMap = Map.empty,anLiveMap = Map.empty, pmCounter = 0, roCounter = 0,spCounter=0,anCounter = 0))

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
      context.become(work(state.copy(pmLiveMap = newMap)))

    case RouterUp(id) =>
      val newMap = state.roLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(roLiveMap = newMap)))

    case SpoutUp(id) =>
      val newMap = state.spLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(spLiveMap = newMap)))

    case AnalysisManagerUp(id) =>
      val newMap = state.anLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(anLiveMap = newMap)))

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

    case RequestSpoutId =>
      sender ! AssignedId(state.spCounter)
      val newCounter = state.spCounter + 1
      context.become(work(state.copy(spCounter = newCounter)))

    case RequestAnalysisId =>
      sender ! AssignedId(state.anCounter)
      val newCounter = state.anCounter + 1
      context.become(work(state.copy(anCounter = newCounter)))

    case unhandled => log.error(s"WatchDog received unknown [$unhandled] message.")
  }

  private def handleTick(state: ActorState): ActorState = {
    checkMapTime(state.pmLiveMap, "Partition Manager")
    checkMapTime(state.roLiveMap, "Router")
    if (state.roLiveMap.size >= minimumRouters &&
        state.pmLiveMap.size >= managerCount &&
        state.spLiveMap.size >= spoutCount &&
        state.anLiveMap.size >= analysisCount ) {
      log.info("Partition managers, Spout, Analysis Manager and Routers have joined cluster.")
      log.debug(s"The cluster was started with [$managerCount] Partition Managers, [$minimumRouters] Routers, 1 Spout and 1 Analysis Manager.")
      state.copy(clusterUp = true)
    } else {
      println(s"Cluster Starting: ${state.roLiveMap.size}/$minimumRouters Routers, ${state.pmLiveMap.size}/$managerCount Partitions, ${state.spLiveMap.size}/$spoutCount Spouts, ${state.anLiveMap.size}/$analysisCount Analysis Managers")
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
    case class SpoutUp(id: Int)
    case class AnalysisManagerUp(id: Int)
    case object ClusterStatusRequest
    case class ClusterStatusResponse(clusterUp: Boolean, pmCounter: Int, roCounter: Int)
    case class AssignedId(id: Int)
    case object RequestPartitionId
    case object RequestRouterId
    case object RequestSpoutId
    case object RequestAnalysisId
    case object RequestPartitionCount
    case class PartitionsCount(count: Int)
  }
  private case class ActorState(
      clusterUp: Boolean,
      pmLiveMap: Map[Int, Long],
      roLiveMap: Map[Int, Long],
      spLiveMap: Map[Int, Long],
      anLiveMap: Map[Int, Long],
      pmCounter: Int,
      roCounter: Int,
      spCounter: Int,
      anCounter: Int
  )
}
