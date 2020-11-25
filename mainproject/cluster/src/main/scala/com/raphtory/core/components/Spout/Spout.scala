package com.raphtory.core.components.Spout

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, Timers}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.Spout.Spout.CommonMessage._
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.Utils
import kamon.Kamon

import scala.concurrent.ExecutionContext


import scala.concurrent.duration._
import scala.language.postfixOps



class Spout(datasource:DataSource) extends Actor with ActorLogging with Timers {
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
    case WorkPlease => sendData(context.sender())
    case unhandled => log.error(s"Unable to handle message [$unhandled].")
  }


  private def processStateCheckMessage(safe: Boolean): Unit = {
    log.debug(s"Spout is handling [StateCheck] message.")
    if (!safe) {
      val sendMessage = ClusterStatusRequest()
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
      Utils.getAllRouterWorkers(roCount).foreach { workerPath =>
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
    try {
      val work =datasource.generateData()
      val message = if (count % 10000 == 0)
        AllocateTrackedTuple(System.currentTimeMillis(),work)
      else
        AllocateTuple(work)
      sender ! message
      recordUpdate()
        if (count % 10000 == 0) println(s"Spout at Message $count")
    }//mediator ! DistributedPubSubMediator.Send(lastRouter, message, localAffinity = false)
    catch {
      case e:NoDataAvailable  => sender ! NoWork
      case e:DataSourceComplete => sender ! DataFinished; println("All data sent")
    }
  }

  def AllocateSpoutTask(duration: FiniteDuration, task: Any): Cancellable = {
    val taskCancellable = context.system.scheduler.scheduleOnce(duration, self, task)
    taskCancellable
  }
}

object Spout {
  object CommonMessage {
    case object StateCheck
    case object IsSafe
    case object WorkPlease
    case object NoWork
    case object SpoutOnline
  }
}
