package com.raphtory.core.components.Router

import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.Router.RouterManager.Message.KeepAlive
import com.raphtory.core.model.communication._

import scala.concurrent.ExecutionContext
//import kamon.metric.CounterMetric
//import kamon.metric.GaugeMetric

import scala.concurrent.duration._
import scala.language.postfixOps

// TODO break object RouterManager { getProps = { routerManager(arg1, arg2...): Props }}
// todo slave type should be Props
final case class RouterManager(routerId: Int, initialManagerCount: Int, slaveType: String)
        extends Actor
        with ActorLogging {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  private val childrenNumber = 10
  private val children = (0 until childrenNumber).map { i =>
    context.actorOf(
            Props(Class.forName(slaveType), routerId, i, initialManagerCount).withDispatcher("router-dispatcher"),
            s"routerWorker_$i"
    )
  }.toList

  private val mediator = DistributedPubSub(context.system).mediator
  mediator ! DistributedPubSubMediator.Put(self)

//  val kGauge: GaugeMetric     = Kamon.gauge("raphtory.benchmarker")
//  val kCounter: CounterMetric = Kamon.counter("raphtory.counters")

  override def preStart(): Unit = {
    log.debug(s"RouterManager [$routerId] is being started.")
    context.system.scheduler.schedule(0 seconds, 10 seconds, self, KeepAlive)
  }

  override def receive: Receive = work(initialManagerCount, children)

  private def work(managerCount: Int, children: List[ActorRef]): Receive = {
    case KeepAlive => processKeepAliveMessage()
    case msg: UpdatedCounter =>
      log.debug(s"RouterManager [$routerId] received [$msg] request.")

      if (managerCount < msg.newValue) {
        log.debug("UpdatedCounter is larger than current managerCount. Bumping managerCount to new value.")
        context.become(work(managerCount, children))
      }
      children.foreach { actorRef =>
        log.debug("Propagating UpdatedCounter to child [{}].", actorRef.path)
        actorRef ! msg
      }
    case unhandled => log.warning(s"cannot handle $unhandled")
  }

  private def processKeepAliveMessage(): Unit = {
    log.debug(s"RouterManager [$routerId] received [KeepAlive] message.")

    val sendMessage = RouterUp(routerId)
    val sendPath    = "/user/WatchDog"

    log.debug(s"Sending DPSM message [$sendMessage] to path [$sendPath].")
    mediator ! DistributedPubSubMediator.Send(sendPath, sendMessage, localAffinity = false)
  }
}

object RouterManager {
  object Message {
    case object KeepAlive
  }
}
