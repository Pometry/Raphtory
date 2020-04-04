package com.raphtory.core.components.ClusterManagement

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.components.PartitionManager.Workers.IngestionWorker
import com.raphtory.core.components.PartitionManager.Archivist
import com.raphtory.core.components.PartitionManager.Reader
import com.raphtory.core.components.PartitionManager.Writer
import com.raphtory.core.components.Router.RouterManager
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.SchedulerUtil
import com.raphtory.core.utils.Utils

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object RaphtoryReplicator {
  def apply(actorType: String, initialManagerCount: Int, routerName: String): RaphtoryReplicator =
    new RaphtoryReplicator(actorType, initialManagerCount, routerName)

  def apply(actorType: String, initialManagerCount: Int): RaphtoryReplicator =
    new RaphtoryReplicator(actorType, initialManagerCount, null)
}

class RaphtoryReplicator(actorType: String, initialManagerCount: Int, routerName: String)
        extends Actor
        with ActorLogging {

  // TODO Make implicit timeouts as secondary (curried), optional implicit parameter
  implicit val timeout: Timeout = 10.seconds

  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  var myId: Int                = -1
  var currentCount: Int        = initialManagerCount
  var actorRef: ActorRef       = _
  var actorRefReader: ActorRef = _

  val mediator: ActorRef = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)

  override def preStart(): Unit = {
    log.debug("Replicator [{}] is being started.")

    scheduleTasks()
  }

  override def postStop(): Unit = {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        SchedulerUtil.cancelTask(key, task)
    }

    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  def receive: Receive = {
    case msg: String if msg == "tick" => processHeartbeatMessage(msg)
    case req: PartitionsCount         => processPartitionsCountRequest(req)
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
          log.error("Failed to retrieve Replicator Id due to timeout.")

          myId = -1

        case e: Exception => log.error("Failed to retrieve Replicator Id due to [{}].", e)
      }
  }

  def processPartitionsCountRequest(req: PartitionsCount): Unit = {
    log.debug(s"Replicator received [{}] request.", req)

    if (req.count > currentCount) {
      currentCount = req.count

      if (actorRef != null)
        actorRef ! UpdatedCounter(currentCount)
      if (actorRefReader != null)
        actorRef ! UpdatedCounter(currentCount)
    }
  }

  def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Replicator Id from WatchDog.")

    val watchDogPath = "/user/WatchDog"

    actorType match {
      case "Partition Manager" =>
        mediator ? DistributedPubSubMediator.Send(watchDogPath, RequestPartitionId, localAffinity = false)
      case "Router" =>
        mediator ? DistributedPubSubMediator.Send(watchDogPath, RequestRouterId, localAffinity = false)
    }
  }

  def giveBirth(assignedId: Int): Unit = {
    log.debug(s"Attempting to instantiate new [{}].", actorType)

    actorType match {
      case "Partition Manager" => createNewPartitionManager(assignedId)
      case "Router" => createNewRouter(assignedId)
    }
  }

  // TODO Expose 10 in range to be a class parameter
  def createNewPartitionManager(assignedId: Int): Unit = {
    log.info(s"Partition Manager $assignedId has come online.")

    var workers: ParTrieMap[Int, ActorRef]       = new ParTrieMap[Int, ActorRef]()
    var storages: ParTrieMap[Int, EntityStorage] = new ParTrieMap[Int, EntityStorage]()

    for (index <- 0 until 10) {
      val storage     = new EntityStorage(index)
      storages.put(index, storage)

      val managerName = s"Manager_${assignedId}_child_$index"
      workers.put(
              index,
              context.system
                .actorOf(Props(new IngestionWorker(index, storage)).withDispatcher("worker-dispatcher"), managerName)
      )
    }

    actorRef = context.system.actorOf(
            Props(new Writer(myId, false, currentCount, workers, storages)).withDispatcher("logging-dispatcher"),
            s"Manager_$myId"
    )

    actorRefReader =
      context.system.actorOf(Props(new Reader(myId, false, currentCount, storages)), s"ManagerReader_$myId")

    _ = context.system.actorOf(Props(new Archivist(0.3, workers, storages)))

  }

  def createNewRouter(assignedId: Int): Unit = {
    log.info(s"Router $assignedId has come online.")

    actorRef = context.system.actorOf(Props(new RouterManager(myId, currentCount, routerName)), "router")
  }


  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in Replicator.")

    val tickCancellable =
      SchedulerUtil.scheduleTask(initialDelay = 2 seconds, interval = 5 seconds, receiver = self, message = "tick")
    scheduledTaskMap.put("tick", tickCancellable)
  }
}
