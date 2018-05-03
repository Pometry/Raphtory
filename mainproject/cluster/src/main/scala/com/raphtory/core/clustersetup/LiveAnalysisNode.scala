package com.raphtory.core.clustersetup

/**
  * Created by Mirate on 29/09/2017.
  */
import akka.actor.Props

case class LiveAnalysisNode(seedLoc: String, name:String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(Class.forName(name)), s"LiveAnalysisManager_$name")
}
