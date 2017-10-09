package com.gwz.dockerexp.caseclass.clustercase

/**
  * Created by Mirate on 29/09/2017.
  */
import com.gwz.dockerexp.Actors.ClusterActors.{DocSvr}
import akka.actor.Props
import com.gwz.dockerexp.Actors.RaphtoryActors._


case class LiveAnalysisNode(seedLoc : String,partitionNumber:String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new LiveAnalysisManager(partitionNumber.toInt)),"LiveAnalysisManager")
}
