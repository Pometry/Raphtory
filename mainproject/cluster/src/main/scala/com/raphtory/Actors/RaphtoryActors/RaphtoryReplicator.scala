package com.raphtory.Actors.RaphtoryActors

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSub
import com.raphtory.caseclass._
import com.raphtory.utils.Utils
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RaphtoryReplicator(actorType:String) extends Actor {
  implicit val timeout: Timeout = Timeout(10 seconds)
  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)

  var myId         = -1
  var currentCount = -1

  var actorRef : ActorRef = null

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
        actorRef = context.system.actorOf(Props(new PartitionManager(myId, false, myId+1)), s"Manager_$myId")
        //context.system.actorOf(Props(new Historian(20, 60, 0.3)))
      }
      case "Router" => actorRef = context.system.actorOf(Props(new RaphtoryRouter(myId,currentCount)), "router")
    }
  }

  override def preStart() {
    context.system.scheduler.schedule(2.seconds,5.seconds,self,"tick")
  }

  def receive : Receive = {
    case PartitionsCount(count) => {
      currentCount = count
      if(actorRef != null)
        actorRef ! UpdatedCounter(currentCount)
    }
    case "tick" => getNewId
    case e => println(s"Received not handled message ${e.getClass}")
  }
}
