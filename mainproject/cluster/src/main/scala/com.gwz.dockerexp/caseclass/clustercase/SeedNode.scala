package com.gwz.dockerexp.caseclass.clustercase

import com.gwz.dockerexp.Actors.ClusterActors.DocSvr

case class SeedNode() extends DocSvr {
  implicit val system = init(List(ipAndPort.hostIP + ":" + ipAndPort.akkaPort))
}
