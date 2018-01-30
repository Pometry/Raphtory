package com.gwz.dockerexp.caseclass.clustercase

import akka.actor.Props
import com.gwz.dockerexp.Actors.ClusterActors.DocSvr

/**
  * Created by Mirate on 29/05/2017.
  */
case class LogicNode(seedLoc: String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new LogicActor(this)), "logic")
}
