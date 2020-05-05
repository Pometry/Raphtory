package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 31/05/2017.
  */
import akka.actor.ActorSystem
import akka.actor.Props
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator

case class RouterNode(seedLoc: String, partitionCount: Int, className: String) extends DocSvr {
  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))

  final val actorName: String = "Routers"
  system.actorOf(
          Props(RaphtoryReplicator(actorType = "Router", initialManagerCount = partitionCount, routerName = className)),
          actorName
  )
}
