package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 31/05/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors._
import com.raphtory.Actors.RaphtoryActors.RaphtoryRouter
import com.raphtory.caseclass.DocSvr

case class RouterNode(seedLoc: String, partitionNumber: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new RaphtoryRouter(partitionNumber.toInt)), "router")
}
