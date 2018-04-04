package com.raphtory.caseclass.clustercase

import akka.actor.Props
import com.raphtory.caseclass.DocSvr
import com.raphtory.Actors.RaphtoryActors.RaphtoryReplicator

import scala.language.postfixOps
import scala.sys.process._

case class ManagerNode(seedLoc: String)
    extends DocSvr {

  implicit val system = init(List(seedLoc))

  system.actorOf(Props(new RaphtoryReplicator()), s"PartitionManager")

  "redis-server --daemonize yes" ! //start redis running on manager partition

}
