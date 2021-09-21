package com.raphtory.core.components.orchestration.componentconnector

import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.spout.{Spout, SpoutAgent}
import akka.pattern.ask
import com.raphtory.core.components.orchestration.raphtoryleader.WatchDog.Message.RequestSpoutId

import scala.concurrent.Future

class SpoutConnector[T](spout: Spout[T]) extends ComponentConnector() {
  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Spout Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestSpoutId, localAffinity = false)

  }

  override def giveBirth(assignedId: Int): Unit = {
    context.system.actorOf(Props(new SpoutAgent(spout)), "Spout")
  }
}
