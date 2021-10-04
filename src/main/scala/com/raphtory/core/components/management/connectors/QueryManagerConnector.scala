package com.raphtory.core.components.management.connectors
import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.cluster.pubsub.DistributedPubSubMediator.SubscribeAck
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, ManagingTask, PointQuery, RangeQuery}
import akka.pattern.ask
import com.raphtory.core.components.querymanager.QueryManager
import com.raphtory.core.components.leader.WatchDog.Message.RequestQueryId

import scala.concurrent.Future
import scala.concurrent.duration.{Duration, MILLISECONDS}

class QueryManagerConnector extends ComponentConnector {
  var queryManager:ActorRef = null
  var monitor:ActorRef = _

  override def receive: Receive = {
    case msg: String if msg == "tick"      => processHeartbeatMessage(msg)
    case _: SubscribeAck                   =>
    case request: LiveQuery                => requesthandler(request)
    case request: PointQuery               => requesthandler(request)
    case request: RangeQuery               => requesthandler(request)
    case taskInfo:ManagingTask             => if(monitor!=null) monitor ! taskInfo
    case x                                 => log.warning(s"Replicator received unknown [{}] message.", x)
  }

  private def requesthandler(request: Any) = {
    monitor = sender()
    if(queryManager!=null){
      queryManager ! request
    }  else {
      context.system.scheduler.scheduleOnce(Duration(2000, MILLISECONDS), self, request)
    }
  }

  override def callTheWatchDog(): Future[Any] = {
    log.debug(s"Attempting to retrieve Query Manager Id from WatchDog.")
    mediator ? DistributedPubSubMediator.Send("/user/WatchDog", RequestQueryId, localAffinity = false)
  }

  override def giveBirth(assignedId: Int): Unit = {
    queryManager = context.system.actorOf(Props[QueryManager].withDispatcher("misc-dispatcher"), s"QueryManager")
  }
}
