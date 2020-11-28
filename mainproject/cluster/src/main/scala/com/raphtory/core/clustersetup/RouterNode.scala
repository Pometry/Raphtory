package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 31/05/2017.
  */
import akka.actor.{ActorSystem, Props}
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator
import com.raphtory.core.components.Router.GraphBuilder

case class RouterNode(seedLoc: String, partitionCount: Int, routerCount:Int, className: String) extends DocSvr {
  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))

  final val actorName: String = "Routers"

  val graphBuilder = Class.forName(className).getConstructor().newInstance().asInstanceOf[GraphBuilder[Any]]
  val routerReplicator = RaphtoryReplicator.apply("Router", partitionCount, routerCount,graphBuilder)
  system.actorOf(Props(routerReplicator), actorName)

}
