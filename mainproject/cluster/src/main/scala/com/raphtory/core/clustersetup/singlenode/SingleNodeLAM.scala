package com.raphtory.core.clustersetup.singlenode

import akka.actor.Props
import com.raphtory.core.actors.{RaphtoryReplicator, SeedActor, WatchDog}
import com.raphtory.core.clustersetup.DocSvr
import com.typesafe.config.{Config, ConfigFactory}

case class SingleNodeLAM(seedLoc:String) extends DocSvr {
  val conf : Config = ConfigFactory.load()
  implicit val system = init(List(seedLoc))
  val partitionNumber = 1
  val minimumRouters = 1
  val routerClassName = "com.raphtory.core.actors.router.RaphtoryRouter"
  val LamClassName = "com.raphtory.core.actors.analysismanager.TestLAM"
  val UpdaterName = "com.raphtory.core.actors.datasource.UpdateGen"

  system.actorOf(Props(Class.forName(LamClassName)), s"LiveAnalysisManager_$LamClassName")
}