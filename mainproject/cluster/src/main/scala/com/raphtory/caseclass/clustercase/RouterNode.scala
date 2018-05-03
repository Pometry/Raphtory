package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 31/05/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors.{RaphtoryReplicator, RaphtoryRouter}
import com.raphtory.caseclass.DocSvr

case class RouterNode(seedLoc: String, className: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(RaphtoryReplicator("Router", className)), s"Routers")
}
