package com.raphtory.core.components.Router

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.model.communication._
import com.raphtory.core.utils.SchedulerUtil
import com.raphtory.core.utils.Utils.getManager
import kamon.Kamon
//import kamon.metric.CounterMetric
//import kamon.metric.GaugeMetric

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

// TODO break object RouterManager { getProps = { routerManager(arg1, arg2...): Props }}
class RouterManager(val routerId: Int, val initialManagerCount: Int, slaveType: String)
        extends Actor
        with ActorLogging {

  private var managerCount: Int = initialManagerCount
  private var count             = 0

  private val children                                               = 10
  private var childMap: ParTrieMap[Int, ActorRef]                    = ParTrieMap[Int, ActorRef]()
  private val scheduledTaskMap: mutable.HashMap[String, Cancellable] = mutable.HashMap[String, Cancellable]()

  final protected val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

//  val kGauge: GaugeMetric     = Kamon.gauge("raphtory.benchmarker")
//  val kCounter: CounterMetric = Kamon.counter("raphtory.counters")

  override def preStart(): Unit = {
    log.debug("RouterManager [{}} is being started.", routerId)

    scheduleTasks()

    for (i <- 0 until children) {
      childMap.put(i, context.actorOf(Props(Class.forName(slaveType), routerId, i, initialManagerCount), s"routerWorker_$i"))
    }
  }

  override def postStop() {
    val allTasksCancelled = scheduledTaskMap.forall {
      case (key, task) =>
        SchedulerUtil.cancelTask(key, task)
    }

    if (!allTasksCancelled) log.warning("Failed to cancel all scheduled tasks post stop.")
  }

  override def receive: Receive = {
    case msg: String if msg == "keep_alive" => processKeepAliveMessage(msg)
    case msg: UpdatedCounter                => processUpdatedCounterRequest(msg)
  }

  private def processKeepAliveMessage(msg: String): Unit = {
    log.debug(s"RouterManager [{}] received [{}] message.", routerId, msg)

    val sendMessage = RouterUp(routerId)
    val sendPath    = "/user/WatchDog"

    log.debug(s"Sending DPSM message [{}] to path [{}].", sendMessage, sendPath)

    mediator ! DistributedPubSubMediator.Send(sendPath, sendMessage, localAffinity = false)
  }

  def processUpdatedCounterRequest(msg: UpdatedCounter): Unit = {
    log.debug(s"RouterManager [{}] received [{}] request.", routerId, msg)

    if (managerCount < msg.newValue) {
      log.debug("UpdatedCounter is larger than current managerCount. Bumping managerCount to new value.")

      managerCount = msg.newValue
    }

    childMap.values.foreach { actorRef =>
      log.debug("Propagating UpdatedCounter to child [{}].", actorRef.path)

      actorRef ! UpdatedCounter(msg.newValue)
    }
  }


  final protected def getManagerCount: Int =
    this.managerCount

  private def scheduleTasks(): Unit = {
    log.debug("Preparing to schedule tasks in RouterManager [{}].", routerId)

    val keepAliveCancellable =
      SchedulerUtil
        .scheduleTask(initialDelay = 0 seconds, interval = 10 seconds, receiver = self, message = "keep_alive")
    scheduledTaskMap.put("keep_alive", keepAliveCancellable)
  }

  def toPartitionManager[T <: GraphUpdate](message: T): Unit =
    mediator ! DistributedPubSubMediator
      .Send(getManager(message.srcID, getManagerCount), message, localAffinity = false)

}
