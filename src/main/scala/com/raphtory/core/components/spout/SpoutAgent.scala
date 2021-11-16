package com.raphtory.core.components.spout

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Timers}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.leader.WatchDog.Message.{ClusterStatusRequest, ClusterStatusResponse, SpoutUp}
import com.raphtory.core.components.spout.SpoutAgent.Message._
import com.raphtory.core.implementations.generic.messaging._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.ClassTag



class SpoutAgent[T:ClassTag](datasource:Spout[T]) extends RaphtoryActor {
  // todo: wvv should assign the dispatcher when create the actor
  //implicit val executionContext: ExecutionContext = context.system.dispatchers.lookup("spout-dispatcher")
  //implicit val executionContext: ExecutionContext = context.system.dispatcher

  override def preStart() {
    log.debug("Spout is being started.")
    mediator ! DistributedPubSubMediator.Put(self)
    context.system.scheduler.schedule(10 seconds,1 seconds, self, StateCheck)
    context.system.scheduler.scheduleOnce(1 seconds, self, IsSafe)
  }

  final override def receive: Receive = work(false)


  private def work(safe: Boolean): Receive = {
    case StateCheck => processStateCheckMessage(safe)
    case ClusterStatusResponse(clusterUp) =>
      context.become(work(clusterUp))
    case IsSafe    => processIsSafeMessage(safe)
    case WorkPlease =>
      processWorkPlease()
    case unhandled => log.error(s"Unable to handle message [$unhandled].")
  }

  private def processWorkPlease():Unit =
      sendData(context.sender())

  private def processStateCheckMessage(safe: Boolean): Unit = {
    log.debug(s"Spout is handling [StateCheck] message.")
    if (!safe) {
      val sendMessage = ClusterStatusRequest
      val sendPath    = "/user/WatchDog"
      log.debug(s"Sending DPSM message [$sendMessage] to path [$sendPath].")
      mediator ! DistributedPubSubMediator.Send(sendPath, sendMessage, localAffinity = false)
    }
  }

  private def processIsSafeMessage(safe: Boolean): Unit = {
    log.debug(s"Spout is handling [IsSafe] message.")
    mediator ! DistributedPubSubMediator.Send("/user/WatchDog", SpoutUp(0), localAffinity = false)
    if (safe) {
      datasource.setupDataSource()
      getAllGraphBuilders().foreach { workerPath =>
        mediator ! DistributedPubSubMediator.Send(
          workerPath,
          SpoutOnline,
          false
        )
      }
    } else{
      context.system.scheduler.scheduleOnce(delay = 1 second, receiver = self, message = IsSafe)
    }

  }


  def sendData(sender:ActorRef): Unit = {
    val tuples = ArrayBuffer[T]()
    for(i<-0 until RaphtoryActor.batchsize) {
      val tuple = datasource.generateData()
      tuple match {
        case Some(work) =>
          tuples += work
        case None if !datasource.isComplete() =>
          if(tuples.isEmpty)
            sender ! NoWork
          else
            sender ! AllocateTuples[T](tuples.toArray)

          return
        case _ =>
          if(tuples.isEmpty)
            sender ! DataFinished
          else
            sender ! AllocateTuples[T](tuples.toArray)

          return
      }
    }//if we make it to the end can send the whole batch
    sender ! AllocateTuples[T](tuples.toArray)



  }

  def AllocateSpoutTask(duration: FiniteDuration, task: Any): Cancellable = {
    val taskCancellable = context.system.scheduler.scheduleOnce(duration, self, task)
    taskCancellable
  }
}

object SpoutAgent {
  object Message {
    case object StateCheck
    case object IsSafe
    case object WorkPlease
    case object NoWork
    case object SpoutOnline
    case object DataFinished
    case class AllocateTuples[T](record:Array[T])
  }
}
