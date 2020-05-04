package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */

import akka.actor.Props
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
case class LiveAnalysisNode(seedLoc: String) extends DocSvr {
  implicit val system = initialiseActorSystem(List(seedLoc))
  system.actorOf(Props[AnalysisManager], s"AnalysisManager")
  AnalysisRestApi(system)
}




