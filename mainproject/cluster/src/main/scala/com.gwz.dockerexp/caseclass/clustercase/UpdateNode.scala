package com.gwz.dockerexp.caseclass.clustercase

/**
  * Created by Mirate on 13/06/2017.
  */
import com.gwz.dockerexp.Actors.ClusterActors.DocSvr
import akka.actor.Props
import com.gwz.dockerexp.Actors.RaphtoryActors._

case class UpdateNode(seedLoc: String, partitionNumber: String,numberOfUpdate:String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new UpdateGen(partitionNumber.toInt,numberOfUpdate.toInt)), "updateGen")
}
