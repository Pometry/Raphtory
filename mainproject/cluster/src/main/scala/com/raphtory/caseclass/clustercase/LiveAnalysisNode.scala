package com.raphtory.caseclass.clustercase

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.Props
import com.raphtory.Actors.RaphtoryActors.LiveAnalysisManager
import com.raphtory.caseclass.DocSvr

case class LiveAnalysisNode(seedLoc: String, name:String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new LiveAnalysisManager(s"/user/LiveAnalysisManager_$name")), s"LiveAnalysisManager_$name")
}
