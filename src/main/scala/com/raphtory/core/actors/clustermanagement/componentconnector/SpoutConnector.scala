package com.raphtory.core.actors.clustermanagement.componentconnector

import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.clustermanagement.WatchDog.Message.{RequestRouterId, RequestSpoutId}
import com.raphtory.core.actors.spout.{Spout, SpoutAgent}
import akka.pattern.ask

import scala.concurrent.Future

class SpoutConnector[T](managerCount: Int, routerCount:Int,spout: Spout[T]) extends ComponentConnector(initialManagerCount = managerCount,initialRouterCount = routerCount) {
  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Spout Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestSpoutId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    context.system.actorOf(Props(new SpoutAgent(spout)), "Spout")
  }
}
