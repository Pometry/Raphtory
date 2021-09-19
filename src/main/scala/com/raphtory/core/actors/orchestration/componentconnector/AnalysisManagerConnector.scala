package com.raphtory.core.actors.orchestration.componentconnector
import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import akka.pattern.ask
import com.raphtory.core.actors.analysismanager.AnalysisManager.Message.ManagingTask
import com.raphtory.core.actors.analysismanager.AnalysisRestApi.message.{LiveAnalysisRequest, RangeAnalysisRequest, ViewAnalysisRequest}
import com.raphtory.core.actors.analysismanager.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.actors.orchestration.clustermanager.WatchDog.Message.{PartitionsCount, RequestAnalysisId}

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS}

class AnalysisManagerConnector() extends ComponentConnector()  {

  var analysismanager:ActorRef = null
  var monitor:ActorRef = _

  override def receive: Receive = {
    case msg: String if msg == "tick"      => processHeartbeatMessage(msg)
    case _: SubscribeAck                   =>
    case request: LiveAnalysisRequest      => requesthandler(request)
    case request: ViewAnalysisRequest      => requesthandler(request)
    case request: RangeAnalysisRequest     => requesthandler(request)
    case taskInfo:ManagingTask             => if(monitor!=null) monitor ! taskInfo
    case x                                 => log.warning(s"Replicator received unknown [{}] message.", x)
  }

  private def requesthandler(request: Any) = {
    monitor = sender()
    if(analysismanager!=null){
      analysismanager ! request
    }  else {
      context.system.scheduler.scheduleOnce(Duration(2000, MILLISECONDS), self, request)
    }
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
