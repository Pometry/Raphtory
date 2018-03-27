package com.raphtory.caseclass.clustercase

import com.raphtory.Actors.ClusterActors.DocSvr

case class SeedNode(seedLoc:String) extends DocSvr {
  implicit val system = init(List(ipAndPort.hostIP + ":" + ipAndPort.akkaPort))
}
