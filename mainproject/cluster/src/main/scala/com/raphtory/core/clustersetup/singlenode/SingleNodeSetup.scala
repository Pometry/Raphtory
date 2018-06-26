package com.raphtory.core.clustersetup.singlenode

import akka.actor.Props
import com.raphtory.core.actors.{RaphtoryReplicator, SeedActor, WatchDog}
import com.raphtory.core.clustersetup.DocSvr
import com.typesafe.config.{Config, ConfigFactory}

case class SingleNodeSetup(seedLoc:String) extends DocSvr {
  val conf : Config = ConfigFactory.load()
  implicit val system = init(List(seedLoc))
  val partitionNumber = 1
  val minimumRouters = 1
  val routerClassName = "com.raphtory.core.actors.router.RaphtoryRouter"
  val LamClassName = "com.raphtory.core.actors.analysismanager.TestLAM"
  val UpdaterName = "com.raphtory.core.actors.datasource.UpdateGen"


  system.actorOf(Props(new SeedActor(this)), "cluster")
  system.actorOf(Props(new WatchDog(partitionNumber,minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router", routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager")), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")
}