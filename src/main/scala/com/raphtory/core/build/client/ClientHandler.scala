package com.raphtory.core.build.client

import akka.actor.Stash
import akka.cluster.pubsub.DistributedPubSubMediator
import akka.pattern.ask
import com.raphtory.core.build.client.ClientHandler.State
import com.raphtory.core.components.akkamanagement.RaphtoryActor
import com.raphtory.core.components.leader.WatchDog.Message.{AssignedId, ClusterStatusRequest, ClusterStatusResponse, RequestBuilderId}
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.model.algorithm.{GraphAlgorithm, GraphFunction, TableFunction}

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

    case PointQuery(name,algorithm,timestamp,windows) =>
      if(state.clusterUp)
        pointQuery(name,algorithm,timestamp,windows)
      else
        stash()

    case RangeQuery(name,algorithm,start,end,increment,windows) =>
      if(state.clusterUp)
        rangeQuery(name,algorithm,start,end,increment,windows)
      else
        stash()

    case LiveQuery(name,algorithm,increment,windows) =>
      if(state.clusterUp)
        liveQuery(name,algorithm,increment,windows)
      else
        stash()
  }

  def pointQuery(name:String,algorithm:(List[GraphFunction],List[TableFunction]), timestamp:Long, windows:List[Long]=List()) = {
    mediator ! DistributedPubSubMediator.Send("/user/QueryManager",
      PointQuery(name,algorithm,timestamp,windows), localAffinity = false)
  }
  def rangeQuery(name:String,algorithm:(List[GraphFunction],List[TableFunction]),start:Long, end:Long, increment:Long,windows:List[Long]=List()) = {
    mediator ! DistributedPubSubMediator.Send("/user/QueryManager",
      RangeQuery(name,algorithm,start,end,increment,windows), localAffinity = false)
  }
  def liveQuery(name:String,algorithm:(List[GraphFunction],List[TableFunction]),increment:Long,windows:List[Long]=List()) = {
    mediator ! DistributedPubSubMediator.Send("/user/QueryManager",
      LiveQuery(name,algorithm,increment,windows), localAffinity = false)
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
