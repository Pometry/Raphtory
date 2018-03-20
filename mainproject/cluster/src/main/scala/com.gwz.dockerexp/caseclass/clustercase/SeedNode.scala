package com.gwz.dockerexp.caseclass.clustercase

import com.gwz.dockerexp.Actors.ClusterActors.DocSvr

case class SeedNode(seedLoc:String) extends DocSvr {
  implicit val system = init(List(ipAndPort.hostIP + ":" + ipAndPort.akkaPort))
}
