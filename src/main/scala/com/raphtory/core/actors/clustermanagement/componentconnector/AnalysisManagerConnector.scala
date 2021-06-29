package com.raphtory.core.actors.clustermanagement.componentconnector
import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import com.raphtory.core.actors.clustermanagement.WatchDog.Message.{PartitionsCount, RequestAnalysisId}
import akka.pattern.ask
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message.{LiveAnalysisRequest, RangeAnalysisRequest, ViewAnalysisRequest}
import com.raphtory.core.actors.analysismanager.tasks.AnalysisTask.Message.StartAnalysis
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS}

class AnalysisManagerConnector(managerCount: Int, routerCount:Int) extends ComponentConnector(initialManagerCount = managerCount,initialRouterCount = routerCount)  {

  var analysismanager:ActorRef = null

  override def receive: Receive = {
    case msg: String if msg == "tick" => processHeartbeatMessage(msg)
    case req: PartitionsCount         => processPartitionsCountRequest(req)
    case _: SubscribeAck              =>
    case request: LiveAnalysisRequest => requesthandler(request)
    case request: ViewAnalysisRequest => requesthandler(request)
    case request: RangeAnalysisRequest => requesthandler(request)
    case x                            => log.warning(s"Replicator received unknown [{}] message.", x)
  }

  private def requesthandler(request: Any) = if(analysismanager!=null){
    println("hello 2")
    analysismanager ! request
  }  else {
    println("hello")
    context.system.scheduler.scheduleOnce(Duration(2000, MILLISECONDS), self, request)
  }

  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Analysis Manager Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestAnalysisId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    AnalysisRestApi(context.system)
    analysismanager = context.system.actorOf(Props[AnalysisManager].withDispatcher("misc-dispatcher"), s"AnalysisManager")
  }
}
