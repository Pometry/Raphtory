package com.raphtory.core.components.akkamanagement.connectors

import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.components.graphbuilder.{BuilderExecutor, GraphBuilder}
import akka.pattern.ask
import com.raphtory.core.components.akkamanagement.RaphtoryActor._
import com.raphtory.core.components.leader.WatchDog.Message.RequestBuilderId
import com.rits.cloning.Cloner

import scala.concurrent.Future

class BuilderConnector[T](graphBuilder: GraphBuilder[T]) extends ComponentConnector() {

  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Builder Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestBuilderId, localAffinity = false)
  }

  val cloner = new Cloner
  def newGraphBuilder()={
    cloner.deepClone(graphBuilder).asInstanceOf[GraphBuilder[Any]]
  }

  override def giveBirth(assignedId: Int): Unit = {
    log.info(s"Builder Machine $assignedId has come online.")
    val startRange = assignedId*buildersPerServer
    val endRange = startRange+buildersPerServer
    (startRange until endRange).map { i =>
      context.system.actorOf(Props(new BuilderExecutor(newGraphBuilder(), i)).withDispatcher("builder-dispatcher"), s"build_$i")
    }.toList

  }
}
