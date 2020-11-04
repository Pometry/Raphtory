package com.raphtory.core.components.Spout

import akka.actor.{Actor, ActorLogging, Cancellable, Timers}
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.IsSafe
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.StartSpout
import com.raphtory.core.components.Spout.SpoutTrait.CommonMessage.StateCheck
import com.raphtory.core.components.Spout.SpoutTrait.DomainMessage
import com.raphtory.core.model.communication._
import kamon.Kamon

import scala.concurrent.ExecutionContext
//import kamon.metric.CounterMetric
//import kamon.metric.GaugeMetric

import scala.concurrent.duration._
import scala.language.postfixOps

// TODO Add val name which sub classes that extend this trait must overwrite
//  e.g. BlockChainSpout val name = "Blockchain Spout"
//  Log.debug that read 'Spout' should then read 'Blockchain Spout'
trait SpoutTrait[Domain <: DomainMessage, Out <: SpoutGoing] extends Actor with ActorLogging with Timers {
  // todo: wvv should assign the dispatcher when create the actor
  // implicit val executionContext: ExecutionContext = context.system.dispatchers.lookup("spout-dispatcher")
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val spoutTuples = Kamon.counter("Raphtory_Spout_Tuples").withTag("actor", self.path.name)
  private var count       = 0
  private var lastRouter = ""
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

  private def work(safe: Boolean,pmCounter:Int,rmCounter:Int): Receive = {
    case StateCheck => processStateCheckMessage(safe)
    case ClusterStatusResponse(clusterUp,pmCounter,rmCounter) =>
      context.become(work(clusterUp,pmCounter,rmCounter))
      context.system.scheduler.scheduleOnce(1 second, self, StateCheck)
    case IsSafe    => processIsSafeMessage(safe,pmCounter,rmCounter)
    case StartSpout => startSpout()
    case x: Domain  => handleDomainMessage(x)
    case unhandled => log.error(s"Unable to handle message [$unhandled].")
  }

  def startSpout(): Unit

  def handleDomainMessage(message: Domain): Unit

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
      self ! StartSpout
      partitionManagers=pmCount
      routers=roCount

    } else
      context.system.scheduler.scheduleOnce(delay = 1 second, receiver = self, message = IsSafe)
  }

  protected def dataFinished():Unit = {
    mediator ! DistributedPubSubMediator.Send(lastRouter, DataFinished, localAffinity = false)
  }

  protected def sendTuple(command: Out): Unit = {
    log.debug(s"The command [$command] received for send.")
    recordUpdate()
    val message =
      if (count % 100 == 0)
        AllocateTrackedTuple(System.currentTimeMillis(), command)
      else
        AllocateTuple(command)
    val mod     = count % (routers * 10)
    lastRouter=s"/user/router/router_${mod/10}_Worker_${mod%10}"
    mediator ! DistributedPubSubMediator.Send(lastRouter, message, localAffinity = false)
  }

  def AllocateSpoutTask(duration: FiniteDuration, task: Any): Cancellable = {
    val taskCancellable = context.system.scheduler.scheduleOnce(duration, self, task)
    taskCancellable
  }
}

object SpoutTrait {
  object CommonMessage {
    case object StartSpout
    case object StateCheck
    case object IsSafe
    case object Next extends BasicDomain
  }
  trait DomainMessage
  sealed trait BasicDomain extends DomainMessage
}
