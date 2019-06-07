package com.raphtory.core.clustersetup

import akka.actor.Props
import com.raphtory.core.components.RaphtoryReplicator
import com.raphtory.core.storage.RaphtoryDBWrite

import scala.language.postfixOps
import scala.sys.process._

case class ManagerNode(seedLoc: String,partitionCount:Int)
    extends DocSvr {

  implicit val system = init(List(seedLoc))

  system.actorOf(Props(RaphtoryReplicator("Partition Manager",partitionCount)), s"PartitionManager")
  val saving: Boolean = System.getenv().getOrDefault("SAVING", "true").trim.toBoolean
  if(saving){
    //Process("cassandra").lineStream //run cassandara in background on manager
    Thread.sleep(5000)
    RaphtoryDBWrite.createDB()
  }


  //"redis-server --daemonize yes" ! //start redis running on manager partition

}
