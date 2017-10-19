package com.gwz.dockerexp.caseclass.clustercase

/**
  * Created by Mirate on 31/05/2017.
  */
import com.gwz.dockerexp.Actors.ClusterActors.{DocSvr}
import akka.actor.Props
import com.gwz.dockerexp.Actors.RaphtoryActors._

case class RouterNode(,partitionNumber:String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new RaphtoryRouter(partitionNumber.toInt)),"router")
}
