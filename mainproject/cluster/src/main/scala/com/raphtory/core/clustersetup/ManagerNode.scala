package com.raphtory.core.clustersetup

import akka.actor.ActorSystem
import akka.actor.Props
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator
import com.raphtory.core.utils.Utils

import scala.language.postfixOps

case class ManagerNode(seedLoc: String, partitionCount: Int) extends DocSvr {

  implicit val system: ActorSystem = initialiseActorSystem(seeds = List(seedLoc))

  final val persistenceEnabled: Boolean = Utils.persistenceEnabled
  final val actorName: String           = "PartitionManager"

  system.actorOf(
          Props(RaphtoryReplicator(actorType = "Partition Manager", initialManagerCount = partitionCount)),
          actorName
  )

  if (persistenceEnabled)
    //Process("cassandra").lineStream //run cassandara in background on manager
    Thread.sleep(5000)

}
