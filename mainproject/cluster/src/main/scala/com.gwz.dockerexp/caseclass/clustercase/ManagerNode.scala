package com.gwz.dockerexp.caseclass.clustercase

import com.gwz.dockerexp.Actors.ClusterActors.DocSvr
import akka.actor.Props
import com.gwz.dockerexp.Actors.RaphtoryActors.PartitionManager
import sys.process._


case class ManagerNode(seedLoc: String, managerID: String, managerCount: String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(
    Props(new PartitionManager(managerID.toInt, false, managerCount.toInt)),
    s"Manager_$managerID")

  "redis-server --daemonize yes" !

  println(s"Manager_$managerID")


}
//.withDispatcher("akka.actor.prio-dispatcher")
