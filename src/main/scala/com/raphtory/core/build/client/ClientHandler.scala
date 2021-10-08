package com.raphtory.core.build.client

import akka.actor.Stash
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.pattern.ask
import com.raphtory.core.build.client.ClientHandler.State
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.leader.WatchDog.Message.{AssignedId, ClusterStatusRequest, ClusterStatusResponse, RequestBuilderId}
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.model.algorithm.GraphAlgorithm

import scala.concurrent.Await
import scala.concurrent.duration._
class ClientHandler extends RaphtoryActor with Stash{

  override def preStart(): Unit =
    scheduleTaskOnce(2 seconds, receiver = self, message = "clusterUp")

  override def receive =
    work(State(false))


  private def work(state: State): Receive = {
    case "clusterUp" =>
      if(!state.clusterUp)
        clusterUp()

    case PointQuery(graphAlgorithm,timestamp,windows) =>
      if(state.clusterUp)
        pointQuery(graphAlgorithm,timestamp,windows)
      else
        stash()

    case RangeQuery(graphAlgorithm,start,end,increment,windows) =>
      if(state.clusterUp)
        rangeQuery(graphAlgorithm,start,end,increment,windows)
      else
        stash()

    case LiveQuery(graphAlgorithm,increment,windows) =>
      if(state.clusterUp)
        liveQuery(graphAlgorithm,increment,windows)
      else
        stash()
  }

  def pointQuery(graphAlgorithm: GraphAlgorithm,timestamp:Long,windows:List[Long]=List()) = {
    mediator ! DistributedPubSubMediator.Send("/user/QueryManager",
      PointQuery(graphAlgorithm,timestamp,windows), localAffinity = false)
  }
  def rangeQuery(graphAlgorithm: GraphAlgorithm,start:Long, end:Long, increment:Long,windows:List[Long]=List()) = {
    mediator ! DistributedPubSubMediator.Send("/user/QueryManager",
      RangeQuery(graphAlgorithm,start,end,increment,windows), localAffinity = false)
  }
  def liveQuery(graphAlgorithm: GraphAlgorithm,increment:Long,windows:List[Long]=List()) = {
    mediator ! DistributedPubSubMediator.Send("/user/QueryManager",
      LiveQuery(graphAlgorithm,increment,windows), localAffinity = false)
  }

  def clusterUp():Unit = {
    val future = mediator ? DistributedPubSubMediator.Send("/user/WatchDog", ClusterStatusRequest, localAffinity = false)
    try{
      val clusterStatus = Await.result(future, timeout.duration).asInstanceOf[ClusterStatusResponse].clusterUp
      if(clusterStatus)
        unstashAll()
      context.become(work(State(clusterStatus)))
    } catch {
      case e:Exception =>
        scheduleTaskOnce(2 seconds, receiver = self, message = "clusterUp")
        context.become(work(State(false)))
    }

  }


}

object ClientHandler {
  private case class State(clusterUp:Boolean)
}
