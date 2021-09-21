package com.raphtory.core.components.orchestration.raphtoryleader

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.ActorRef
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent, MemberExited, MemberRemoved, MemberUp, UnreachableMember}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import akka.event.LoggingReceive
import com.raphtory.core.components.RaphtoryActor
import com.raphtory.core.components.RaphtoryActor.{analysisCount, builderServers, buildersPerServer, partitionServers, spoutCount, totalBuilders, totalPartitions}
import com.raphtory.core.components.orchestration.raphtoryleader.WatchDog.ActorState
import com.raphtory.core.components.orchestration.raphtoryleader.WatchDog.Message._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

class WatchDog() extends RaphtoryActor {

  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val maxTimeInMillis = 30000

  val cluster: Cluster = Cluster(context.system)
  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit = {
    log.debug("WatchDog is being started.")
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent], classOf[UnreachableMember])

    context.system.scheduler.scheduleOnce(10.seconds, self, Tick)
  }

  override def receive: Receive =
    work(ActorState(clusterUp = false, pmLiveMap = Map.empty, gbLiveMap = Map.empty,spLiveMap = Map.empty,anLiveMap = Map.empty, pmCounter = 0, roCounter = 0,spCounter=0,anCounter = 0))

  private def work(state: ActorState): Receive = LoggingReceive {
    case Tick =>
      if(!state.clusterUp) {
        val newState = handleTick(state)
        context.become(work(newState))
      }
      context.system.scheduler.scheduleOnce(10.seconds, self, Tick)

    case ClusterStatusRequest =>
      sender ! ClusterStatusResponse(state.clusterUp)

    case RequestPartitionCount =>
      log.debug(s"Sending Partition Manager count [${state.pmCounter}].")
      sender ! PartitionsCount(state.pmCounter)

    case PartitionUp(id) =>
      val newMap = state.pmLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(pmLiveMap = newMap)))

    case BuilderUp(id) =>
      val newMap = state.gbLiveMap + (id -> System.currentTimeMillis())
      context.become(work(state.copy(gbLiveMap = newMap)))

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
      //mediator ! DistributedPubSubMediator.Publish(partitionsTopic, PartitionsCount(newCounter))

    case RequestBuilderId =>
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

     //TODO do something with these
    case evt: MemberUp          =>
    case evt: MemberRemoved     =>
    case evt: UnreachableMember =>
    case evt: MemberExited      =>

    case unhandled => log.error(s"WatchDog received unknown [$unhandled] message.")
  }

  private def handleTick(state: ActorState): ActorState = {
    checkMapTime(state.pmLiveMap, "Partition Manager")
    checkMapTime(state.gbLiveMap, "Builder")
    if (state.gbLiveMap.size >= totalBuilders &&
        state.pmLiveMap.size >= partitionServers &&
        state.spLiveMap.size >= spoutCount &&
        state.anLiveMap.size >= analysisCount ) {
      println(s"Cluster Started: ${totalBuilders} Graph Builders, $totalPartitions Partitions, $spoutCount Spout, $analysisCount Analysis Manager")
      state.copy(clusterUp = true)
    } else {
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
    case class BuilderUp(id: Int)
    case class PartitionUp(id: Int)
    case class SpoutUp(id: Int)
    case class AnalysisManagerUp(id: Int)
    case object ClusterStatusRequest
    case class ClusterStatusResponse(clusterUp: Boolean)
    case class AssignedId(id: Int)
    case object RequestPartitionId
    case object RequestBuilderId
    case object RequestSpoutId
    case object RequestAnalysisId
    case object RequestPartitionCount
    case class PartitionsCount(count: Int)
  }
  private case class ActorState(
                                 clusterUp: Boolean,
                                 pmLiveMap: Map[Int, Long],
                                 gbLiveMap: Map[Int, Long],
                                 spLiveMap: Map[Int, Long],
                                 anLiveMap: Map[Int, Long],
                                 pmCounter: Int,
                                 roCounter: Int,
                                 spCounter: Int,
                                 anCounter: Int
  )
}
