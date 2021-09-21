package com.raphtory.core.actors.orchestration.componentconnector

import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.graphbuilder.{GraphBuilder, BuilderExecutor}
import akka.pattern.ask
import com.raphtory.core.actors.RaphtoryActor.workersPerRouter
import com.raphtory.core.actors.orchestration.clustermanager.WatchDog.Message.RequestRouterId

import scala.concurrent.Future

class RouterConnector[T](graphBuilder: GraphBuilder[T]) extends ComponentConnector() {

  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Router Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestRouterId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    log.info(s"Router $assignedId has come online.")
    val startRange = assignedId*workersPerRouter
    val endRange = startRange+workersPerRouter
    (startRange until endRange).map { i =>
      val tempGraphBuilder = Class.forName(graphBuilder.getClass.getCanonicalName).getConstructor().newInstance().asInstanceOf[GraphBuilder[T]]
      context.system.actorOf(Props(new BuilderExecutor(tempGraphBuilder, i)).withDispatcher("router-dispatcher"), s"route_$i")
    }.toList

  }
}
