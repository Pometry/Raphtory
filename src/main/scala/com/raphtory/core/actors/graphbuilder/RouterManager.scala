package com.raphtory.core.actors.graphbuilder

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.actors.RaphtoryActor.workersPerRouter
import com.raphtory.core.actors.graphbuilder.RouterManager.Message._
import com.raphtory.core.actors.orchestration.clustermanager.WatchDog.Message.RouterUp
import com.raphtory.core.model.communication._

import scala.concurrent.ExecutionContext
//import kamon.metric.CounterMetric
//import kamon.metric.GaugeMetric

import scala.concurrent.duration._
import scala.language.postfixOps

// TODO break object RouterManager { getProps = { routerManager(arg1, arg2...): Props }}
// todo slave type should be Props
final case class RouterManager[T](routerId: Int, graphBuilder: GraphBuilder[T])
        extends RaphtoryActor {
  implicit val executionContext: ExecutionContext = context.system.dispatcher

  val startRange = routerId*workersPerRouter
  val endRange = startRange+workersPerRouter
  private val children = (startRange until endRange).map { i =>
    val tempGraphBuilder = Class.forName(graphBuilder.getClass.getCanonicalName).getConstructor().newInstance().asInstanceOf[GraphBuilder[T]]
    context.system.actorOf(
            Props(new RouterWorker(tempGraphBuilder, i)).withDispatcher("router-dispatcher"),
            s"route_$i"
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

  override def receive: Receive = work(children)

  private def work(children: List[ActorRef]): Receive = {
    case KeepAlive => processKeepAliveMessage()
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
