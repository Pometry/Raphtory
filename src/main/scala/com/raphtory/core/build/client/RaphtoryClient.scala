package com.raphtory.core.build.client

import akka.actor.{ActorRef, Props}
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.leader.WatchDog.Message.SpoutUp
import com.raphtory.core.components.akkamanagement.ComponentFactory
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.model.algorithm.GraphAlgorithm

class RaphtoryClient(leader:String,port:Int) {
  val system = ComponentFactory.initialiseActorSystem(List(leader),port)
  val handler = system.actorOf(Props(new ClientHandler),"clientHandler")


  def pointQuery(graphAlgorithm: GraphAlgorithm,timestamp:Long,windows:List[Long]=List()) = {
    handler ! PointQuery(graphAlgorithm,timestamp,windows)
  }
  def rangeQuery(graphAlgorithm: GraphAlgorithm,start:Long, end:Long, increment:Long,windows:List[Long]=List()) = {
    handler ! RangeQuery(graphAlgorithm,start,end,increment,windows)
  }
  def liveQuery(graphAlgorithm: GraphAlgorithm,increment:Long,windows:List[Long]=List()) = {
    handler ! LiveQuery(graphAlgorithm,increment,windows)
  }

}
