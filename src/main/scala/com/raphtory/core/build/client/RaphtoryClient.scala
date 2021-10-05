package com.raphtory.core.build.client

import akka.actor.ActorRef
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import com.raphtory.core.components.leader.WatchDog.Message.SpoutUp
import com.raphtory.core.components.management.ComponentFactory
import com.raphtory.core.components.querymanager.QueryManager.Message.{LiveQuery, PointQuery, RangeQuery}
import com.raphtory.core.model.algorithm.GraphAlgorithm

class RaphtoryClient(leader:String,port:Int) {
  val system = ComponentFactory.initialiseActorSystem(List(leader),port)
  val mediator: ActorRef = DistributedPubSub(system).mediator

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
}
