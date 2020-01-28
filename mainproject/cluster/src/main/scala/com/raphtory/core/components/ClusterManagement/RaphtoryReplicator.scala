package com.raphtory.core.components.ClusterManagement

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.components.PartitionManager.Archivist
import com.raphtory.core.components.PartitionManager.Reader
import com.raphtory.core.components.PartitionManager.Workers.IngestionWorker
import com.raphtory.core.components.PartitionManager.Writer
import com.raphtory.core.components.Router.RaphtoryRouter
import com.raphtory.core.model.communication._
import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.Utils

import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

object RaphtoryReplicator {

  // Router instantiation
  def apply(actorType: String, initialManagerCount: Int, routerName: String): RaphtoryReplicator = {
    new RaphtoryReplicator(actorType, initialManagerCount, routerName)
  }

  // PartitionManager instantiation
  def apply(actorType: String, initialManagerCount: Int): RaphtoryReplicator = {
    new RaphtoryReplicator(actorType, initialManagerCount, null)
  }
}

class RaphtoryReplicator(actorType: String, initialManagerCount: Int, routerName: String) extends Actor {

  implicit val timeout: Timeout = 10.seconds
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)

  var myId = -1
  var currentCount = initialManagerCount

  var actorRef: ActorRef = null
  var actorRefReader: ActorRef = null

  def getNewId() = {
    if (myId == -1) {
      try {
        val future = callTheWatchDog() //get the future object from the watchdog requesting either a PM id or Router id
        myId = Await.result(future, timeout.duration).asInstanceOf[AssignedId].id
        giveBirth(myId) //create the child actor (PM or Router)
      }
      catch {
        case e: java.util.concurrent.TimeoutException => {
          myId = -1
          println("Request for ID Timed Out")
        }
      }
    }
  }

  def callTheWatchDog(): Future[Any] = {
    actorType match {
      case "Partition Manager" => mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionId, false)
      case "Router" => mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestRouterId, false)
    }
  }

  def giveBirth(assignedId: Int): Unit = {
    println(s"MyId is $assignedId")
    actorType match {
      case "Partition Manager" => {
        var workers: ParTrieMap[Int, ActorRef] = new ParTrieMap[Int, ActorRef]()
        var storages: ParTrieMap[Int, EntityStorage] = new ParTrieMap[Int, EntityStorage]()
        for (i <- 0 until 10) { //create threads for writing
          val storage = new EntityStorage(i)
          storages.put(i, storage)
          workers.put(i, context.system.actorOf(Props(new IngestionWorker(i, storage)).withDispatcher("worker-dispatcher"), s"Manager_${assignedId}_child_$i"))
        }
        actorRef = context.system.actorOf(Props(new Writer(myId, false, currentCount, workers, storages)).withDispatcher("logging-dispatcher"), s"Manager_$myId")
        actorRefReader = context.system.actorOf(Props(new Reader(myId, false, currentCount, storages)), s"ManagerReader_$myId")
        context.system.actorOf(Props(new Archivist(0.3, workers, storages)))
      }

      case "Router" => {
        actorRef = context.system.actorOf(Props(new RaphtoryRouter(myId, currentCount, routerName)), "router")
      }
    }
  }

  override def preStart() {
    context.system.scheduler.schedule(2.seconds, 5.seconds, self, "tick")
  }

  def receive: Receive = {
    case PartitionsCount(count) => {
      if (count > currentCount) {
        currentCount = count
        if (actorRef != null)
          actorRef ! UpdatedCounter(currentCount)
        if (actorRefReader != null)
          actorRef ! UpdatedCounter(currentCount)
      }
    }
    case "tick" => getNewId
    //    case GetNetworkSize() => {
    //      println("GetNetworkSize" + actorRefReader != null)
    //      if (actorRefReader != null)
    //        sender() ! actorRefReader ? GetNetworkSize
    //    }
    case e: SubscribeAck =>
    case e => println(s"Received not handled message ${e.getClass}")
  }
}
