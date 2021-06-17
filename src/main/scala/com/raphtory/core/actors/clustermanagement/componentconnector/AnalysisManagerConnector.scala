package com.raphtory.core.actors.clustermanagement.componentconnector
import akka.actor.Props
import akka.cluster.pubsub.DistributedPubSubMediator
import com.raphtory.core.actors.clustermanagement.WatchDog.Message.{RequestAnalysisId}
import akka.pattern.ask
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}

import scala.concurrent.Future

class AnalysisManagerConnector(managerCount: Int, routerCount:Int) extends ComponentConnector(initialManagerCount = managerCount,initialRouterCount = routerCount)  {
  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Analysis Manager Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestAnalysisId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    AnalysisRestApi(context.system)
    context.system.actorOf(Props[AnalysisManager].withDispatcher("misc-dispatcher"), s"AnalysisManager")
  }
}
