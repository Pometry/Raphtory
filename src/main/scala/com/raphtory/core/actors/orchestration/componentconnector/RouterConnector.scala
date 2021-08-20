package com.raphtory.core.actors.orchestration.componentconnector

import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.graphbuilder.{GraphBuilder, RouterManager}
import akka.pattern.ask
import com.raphtory.core.actors.orchestration.clustermanager.WatchDog.Message.RequestRouterId

import scala.concurrent.Future

class RouterConnector[T](managerCount: Int, routerCount:Int,graphBuilder: GraphBuilder[T]) extends ComponentConnector(initialManagerCount = managerCount,initialRouterCount = routerCount) {

  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Router Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestRouterId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    log.info(s"Router $assignedId has come online.")

    actorRef = context.system.actorOf(
      Props(new RouterManager(myId, currentCount, routerCount, graphBuilder)).withDispatcher("misc-dispatcher"),
      "router"
    )
  }
}
