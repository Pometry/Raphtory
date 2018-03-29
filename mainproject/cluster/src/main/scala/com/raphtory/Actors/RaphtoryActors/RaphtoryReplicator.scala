package com.raphtory.Actors.RaphtoryActors

import java.net.InetAddress

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSub
import com.raphtory.caseclass._
import com.raphtory.utils.Utils
import akka.pattern.ask
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class RaphtoryReplicator extends Actor {

  val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)
  mediator ! DistributedPubSubMediator.Subscribe(Utils.partitionsTopic, self)

  var myId         = -1
  var currentCount = -1

  var watchDogIp = ""
  var actorRef : ActorRef = null

  //Utils.watchDogSelector(context) ! RequestPartitionId
  def getNewId() = {
    if (myId == -1) {
      try {
        implicit val timeout: Timeout = Timeout(10 seconds)
        val future = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestPartitionId, false)
        myId = Await.result(future, timeout.duration).asInstanceOf[AssignedId].id
        actorRef = context.system.actorOf(
          Props(new PartitionManager(myId, false, myId+1)),
          s"Manager_$myId")
      }
      catch {
        case e: java.util.concurrent.TimeoutException => {
          myId = -1
          println("Request for ID Timed Out")
        }
      }
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
    case "tick" => {
      getNewId()
    }
    case _ => println("Received not handled message")
  }
}
