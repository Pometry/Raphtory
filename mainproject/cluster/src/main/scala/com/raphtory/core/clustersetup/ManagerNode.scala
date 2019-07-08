package com.raphtory.core.clustersetup

import akka.actor.Props
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator
import com.raphtory.core.storage.RaphtoryDBWrite
import com.raphtory.core.utils.Utils

import scala.language.postfixOps
import scala.sys.process._

case class ManagerNode(seedLoc: String,partitionCount:Int)
    extends DocSvr {

  implicit val system = init(List(seedLoc))

  system.actorOf(Props(RaphtoryReplicator("Partition Manager",partitionCount)), s"PartitionManager")
  val saving: Boolean = Utils.saving
  if(saving){
    //Process("cassandra").lineStream //run cassandara in background on manager
    Thread.sleep(5000)
    RaphtoryDBWrite.createDB()
    RaphtoryDBWrite.clearDB()
  }


  //"redis-server --daemonize yes" ! //start redis running on manager partition

}
