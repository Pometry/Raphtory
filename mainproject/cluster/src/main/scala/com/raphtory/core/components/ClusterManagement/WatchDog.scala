package com.raphtory.core.components.ClusterManagement

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.SchedulerUtil
import com.raphtory.core.utils.Utils

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration._

class WatchDog(managerCount: Int, minimumRouters: Int) extends Actor with ActorLogging {

  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()
  implicit val executionContext = context.system.dispatchers.lookup("misc-dispatcher")

  var clusterUp = false
  val maxTime   = 30000
  var pmCounter = 0
  var roCounter = 0

  var PMKeepAlive: TrieMap[Int, Long]     = TrieMap[Int, Long]()
  var RouterKeepAlive: TrieMap[Int, Long] = TrieMap[Int, Long]()

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

  override def preStart(): Unit = {
    log.debug("WatchDog is being started.")

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
    case msg: String if msg == "tick"                => processHeartbeatMessage(msg)
    case msg: String if msg == "refreshManagerCount" => processRefreshManagerCountMessage(msg)
    case req: ClusterStatusRequest                   => processClusterStatusRequest(req)
    case req: RequestPartitionCount                  => processRequestPartitionCountRequest(req)
    case req: PartitionUp                            => processPartitionUpRequest(req)
    case req: RouterUp                               => processRouterUpRequest(req)
    case req: RequestPartitionId                     => processRequestPartitionIdRequest(req)
    case req: RequestRouterId                        => processRequestRouterIdRequest(req)
    case x                                           => log.warning("WatchDog received unknown [{}] message.", x)
  }

  // TODO Simple srcID hash at the moment
  def getManager(srcId: Int): String = s"/user/Manager_${srcId % managerCount}"

  private def processHeartbeatMessage(msg: String): Unit = {
    log.debug(s"WatchDog received [{}] message.", msg)

    checkMapTime(PMKeepAlive)
    checkMapTime(RouterKeepAlive)

    if (!clusterUp)
      if (RouterKeepAlive.size >= minimumRouters)
        if (PMKeepAlive.size == managerCount) {
          clusterUp = true
          log.info("Partition managers and min. number of Routers have joined cluster.")
          log.debug(
                  "The cluster was started with [{}] Partition Managers and [{}] >= Routers.",
                  managerCount,
                  minimumRouters
          )
        }
  }

  private def processRefreshManagerCountMessage(msg: String): Unit = {
    log.debug(s"WatchDog received [{}] message.", msg)

    mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(pmCounter))
  }

  private def processClusterStatusRequest(req: ClusterStatusRequest): Unit = {
    log.debug(s"WatchDog received [{}] request.", req)

    sender ! ClusterStatusResponse(clusterUp,pmCounter,roCounter)
  }

  private def processRequestPartitionCountRequest(req: RequestPartitionCount): Unit = {
    log.debug(s"WatchDog received [{}] request.", req)

    log.debug(s"Sending Partition Manager count [{}].", pmCounter)
    sender ! PartitionsCountResponse(pmCounter)
  }

  private def processPartitionUpRequest(req: PartitionUp): Unit = {
    log.debug(s"WatchDog received [{}] request.", req)

    mapHandler(req.id, PMKeepAlive, "Partition Manager")
  }

  private def processRouterUpRequest(req: RouterUp): Unit = {
    log.debug(s"WatchDog received [{}] request.", req)

    mapHandler(req.id, RouterKeepAlive, "Router")
  }

  private def processRequestPartitionIdRequest(req: RequestPartitionId): Unit = {
    log.debug(s"Sending assigned id [{}] for new partition manager to Replicator.", pmCounter)

    sender() ! AssignedId(pmCounter)
    pmCounter += 1

    log.debug("Propagating the new total partition managers [{}] to all the subscribers.", pmCounter)
    mediator ! DistributedPubSubMediator.Publish(Utils.partitionsTopic, PartitionsCount(pmCounter))
  }

  private def processRequestRouterIdRequest(req: RequestRouterId): Unit = {
    log.debug(s"Sending assigned id [{}] for new router to Replicator.", roCounter)

    sender ! AssignedId(roCounter)
    roCounter += 1
  }

  private def checkMapTime(map: TrieMap[Int, Long]): Unit =
    map.foreach {
      case (pmId, startTime) =>
        if (startTime + maxTime <= System.currentTimeMillis())
          log.debug("Partition manager [{}] not responding since [{}].", pmId, Utils.unixToTimeStamp(startTime))
    }

  private def mapHandler(id: Int, map: TrieMap[Int, Long], mapType: String): Unit = {
    log.debug(s"Checking [{}] status for id [{}].", mapType, id)

    map.putIfAbsent(id, System.currentTimeMillis()) match {
      case Some(_) => map.update(id, System.currentTimeMillis())
      case _ =>
        log.debug(
                "The [{}] for id [{}] has started. Keep alive will be sent at [{}].",
                mapType,
                id,
                Utils.nowTimeStamp()
        )
    }
  }

  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in WatchDog.")

    val tickCancellable = SchedulerUtil.scheduleTask(2 seconds, 10 seconds, self, "tick")
    scheduledTaskMap.put("tick", tickCancellable)

    val refreshManagerCountCancellable =
      SchedulerUtil.scheduleTask(3 minutes, 1 minute, self, "refreshManagerCount")
    scheduledTaskMap.put("refreshManagerCount", refreshManagerCountCancellable)
  }

}
