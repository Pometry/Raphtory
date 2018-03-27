package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 11/07/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.ClusterActors.{ClusterUpActor, DocSvr}

case class ClusterUpNode(seedLoc: String, partitionNumber: String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new ClusterUpActor(partitionNumber.toInt)), "ClusterUp")
}
