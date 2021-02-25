package com.raphtory.core.actors.Spout

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Timers}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.ClusterManagement.WatchDog.Message.{ClusterStatusRequest, ClusterStatusResponse}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.Spout.SpoutAgent.CommonMessage._
import com.raphtory.core.model.communication._
import kamon.Kamon

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps



class SpoutAgent(datasource:Spout[Any]) extends RaphtoryActor {
  // todo: wvv should assign the dispatcher when create the actor
  implicit val executionContext: ExecutionContext = context.system.dispatchers.lookup("spout-dispatcher")
  //implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val spoutTuples = Kamon.counter("Raphtory_Spout_Tuples").withTag("actor", self.path.name)
  private var count       = 0
  private var partitionManagers = 0
  private var routers = 0

  private def recordUpdate(): Unit = {
    spoutTuples.increment()
    count += 1
  }

  private val mediator = DistributedPubSub(context.system).mediator

  override def preStart() {
    log.debug("Spout is being started.")
    mediator ! DistributedPubSubMediator.Put(self)
    context.system.scheduler.scheduleOnce(7 seconds, self, StateCheck)
    context.system.scheduler.scheduleOnce(1 seconds, self, IsSafe)
  }

  final override def receive: Receive = work(false,1,1)


  private def work(safe: Boolean, pmCounter:Int, rmCounter:Int): Receive = {
    case StateCheck => processStateCheckMessage(safe)
    case ClusterStatusResponse(clusterUp,pmCounter,rmCounter) =>
      context.become(work(clusterUp,pmCounter,rmCounter))
      context.system.scheduler.scheduleOnce(1 second, self, StateCheck)
    case IsSafe    => processIsSafeMessage(safe,pmCounter,rmCounter)
    case WorkPlease => processWorkPlease()
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

  private def processIsSafeMessage(safe: Boolean,pmCount:Int,roCount:Int): Unit = {
    log.debug(s"Spout is handling [IsSafe] message.")
    if (safe) {
      datasource.setupDataSource()
      partitionManagers=pmCount
      routers=roCount
      getAllRouterWorkers(roCount).foreach { workerPath =>
        mediator ! DistributedPubSubMediator.Send(
          workerPath,
          SpoutOnline,
          false
        )
      }
      println(s"Number of routers: $routers")
      println(s"Number of partitions: $partitionManagers")

    } else
      context.system.scheduler.scheduleOnce(delay = 1 second, receiver = self, message = IsSafe)
  }


  def sendData(sender:ActorRef): Unit = {

      datasource.generateData() match {
        case Some(work) =>
          val message = AllocateTuple(work)
          sender ! message
          recordUpdate()
          if (count % 10000 == 0) println(s"Spout at Message $count")
        case None if !datasource.isComplete() =>   sender ! NoWork
        case _ => sender ! DataFinished
    }
  }

  def AllocateSpoutTask(duration: FiniteDuration, task: Any): Cancellable = {
    val taskCancellable = context.system.scheduler.scheduleOnce(duration, self, task)
    taskCancellable
  }
}

object SpoutAgent {
  object CommonMessage {
    case object StateCheck
    case object IsSafe
    case object WorkPlease
    case object NoWork
    case object SpoutOnline
    case object DataFinished
  }
}
