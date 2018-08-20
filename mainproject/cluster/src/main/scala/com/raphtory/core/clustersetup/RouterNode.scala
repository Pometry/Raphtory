package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 31/05/2017.
  */
import akka.actor.Props
import com.raphtory.core.actors.RaphtoryReplicator

case class RouterNode(seedLoc: String, className: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(RaphtoryReplicator("Router", className)), s"Routers")
}
