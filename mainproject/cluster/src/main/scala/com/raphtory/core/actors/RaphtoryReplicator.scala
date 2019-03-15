package com.raphtory.core.actors

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSub
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils
import akka.pattern.ask
import akka.util.Timeout
import com.raphtory.core.actors.partitionmanager.Archivist.Archivist
import com.raphtory.core.actors.partitionmanager.Reader.PartitionReader
import com.raphtory.core.actors.partitionmanager.Writer.PartitionWriter
import com.raphtory.core.actors.router.TraditionalRouter.RaphtoryRouter

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
object RaphtoryReplicator {

  // Router instantiation
  def apply(actorType : String, initialManagerCount:Int, routerName : String) = {
    new RaphtoryReplicator(actorType, initialManagerCount, routerName)
  }

  // PartitionManager instantiation
  def apply(actorType : String,initialManagerCount:Int) = {
    new RaphtoryReplicator(actorType, initialManagerCount,null)
  }
}

class RaphtoryReplicator(actorType:String, initialManagerCount:Int, routerName : String) extends Actor {
  implicit val timeout: Timeout = Timeout(10 seconds)
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)

  var myId         = -1
  var currentCount = initialManagerCount

  var actorRef : ActorRef = null
  var actorRefReader : ActorRef = null

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

  def callTheWatchDog():Future[Any] = {
    actorType match {
      case "Partition Manager" => mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionId, false)
      case "Router" => mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestRouterId, false)
    }
  }

  def giveBirth(assignedId:Int): Unit ={
    println(s"MyId is $assignedId")
    actorType match {
      case "Partition Manager" => {

        actorRef = context.system.actorOf(Props(new PartitionWriter(myId, false, currentCount)), s"Manager_$myId")
        actorRefReader = context.system.actorOf(Props(new PartitionReader(myId, false, currentCount)), s"ManagerReader_$myId")
        context.system.actorOf(Props(new Archivist(0.3)))
      }

      case "Router" => {
        actorRef = context.system.actorOf(Props(new RaphtoryRouter(myId, currentCount,routerName)), "router")
      }
    }
  }

  override def preStart() {
    context.system.scheduler.schedule(2.seconds,5.seconds,self,"tick")
  }

  def receive : Receive = {
    case PartitionsCount(count) => {
      if(count>currentCount) {
        currentCount = count
        if (actorRef != null)
          actorRef ! UpdatedCounter(currentCount)
        if (actorRefReader != null)
          actorRef ! UpdatedCounter(currentCount)
      }
    }
    case "tick" => getNewId
    case GetNetworkSize() => {
      println("GetNetworkSize" + actorRefReader != null)
      if (actorRefReader != null)
        sender() ! actorRefReader ? GetNetworkSize
    }
    case e => println(s"Received not handled message ${e.getClass}")
  }
}
