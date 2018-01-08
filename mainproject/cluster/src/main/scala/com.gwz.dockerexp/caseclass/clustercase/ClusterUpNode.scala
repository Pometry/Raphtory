package com.gwz.dockerexp.caseclass.clustercase

/**
  * Created by Mirate on 11/07/2017.
  */
import com.gwz.dockerexp.Actors.ClusterActors.{ClusterUpActor, DocSvr}
import akka.actor.Props

case class ClusterUpNode(seedLoc: String, partitionNumber: String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new ClusterUpActor(partitionNumber.toInt)), "ClusterUp")
}
