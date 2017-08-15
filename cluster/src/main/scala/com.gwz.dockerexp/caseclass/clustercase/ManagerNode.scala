package com.gwz.dockerexp.caseclass.clustercase

import com.gwz.dockerexp.Actors.ClusterActors.DocSvr
import akka.actor.Props
import com.gwz.dockerexp.Actors.RaphtoryActors.PartitionManager

case class ManagerNode(seedLoc : String,managerID:String,managerCount:String) extends DocSvr {
  implicit val system = init(List(seedLoc))
  system.actorOf(Props(new PartitionManager(managerID.toInt,false,managerCount.toInt)),s"Manager_$managerID")
  println(s"Manager_$managerID")
}
//.withDispatcher("akka.actor.prio-dispatcher")