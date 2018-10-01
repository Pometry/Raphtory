package com.raphtory.core.clustersetup

import akka.actor.Props
import com.raphtory.core.actors.RaphtoryReplicator
import com.raphtory.core.storage.RaphtoryDBWrite

import scala.language.postfixOps
import scala.sys.process._

case class ManagerNode(seedLoc: String,partitionCount:Int)
    extends DocSvr {

  implicit val system = init(List(seedLoc))

  system.actorOf(Props(RaphtoryReplicator("Partition Manager",partitionCount)), s"PartitionManager")

  "cassandra &" ! //run cassandara in background on manager

  Thread.sleep(20000)
  RaphtoryDBWrite.createDB()

  //"redis-server --daemonize yes" ! //start redis running on manager partition

}
