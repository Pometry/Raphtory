package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors._
import com.raphtory.Actors.ClusterActors.DocSvr
import com.raphtory.Actors.RaphtoryActors.LiveAnalysisManager

case class LiveAnalysisNode(seedLoc: String, partitionNumber: String,name:String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new LiveAnalysisManager(partitionNumber.toInt,s"/user/LiveAnalysisManager_$name")), s"LiveAnalysisManager_$name")
}
