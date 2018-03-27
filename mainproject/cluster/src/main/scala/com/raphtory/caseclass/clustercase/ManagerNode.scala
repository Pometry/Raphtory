package com.raphtory.caseclass.clustercase

import akka.actor.Props
import com.raphtory.Actors.ClusterActors.DocSvr
import com.raphtory.Actors.RaphtoryActors.PartitionManager
import scala.language.postfixOps
import scala.sys.process._


case class ManagerNode(seedLoc: String, managerID: String, managerCount: String)
    extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(
    Props(new PartitionManager(managerID.toInt, false, managerCount.toInt)),
    s"Manager_$managerID")

  "redis-server --daemonize yes" ! //start redis running on manager partition

  println(s"Manager_$managerID")


}
//.withDispatcher("akka.actor.prio-dispatcher")
