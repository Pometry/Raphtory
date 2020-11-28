package com.raphtory.core.clustersetup.singlenode

import akka.actor.Props
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.clustersetup.DocSvr
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, SeedActor, WatchDog, WatermarkManager}
import com.raphtory.core.components.Router.GraphBuilder
import com.typesafe.config.{Config, ConfigFactory}

import scala.language.postfixOps

// TODO migrate to object props
case class SingleNodeSetup(
    seedLoc: String,
    routerClassName: String,
    UpdaterName: String
) extends DocSvr {
  val conf: Config    = ConfigFactory.load()
  implicit val system = initialiseActorSystem(List(seedLoc))
  AnalysisRestApi(system)
  system.actorOf(Props(new SeedActor(this)), "cluster")
  system.actorOf(Props(new WatchDog(1, 1)), "WatchDog")
  system.actorOf(Props(new WatermarkManager(managerCount = 1)),"WatermarkManager")

  val graphBuilder = Class.forName(routerClassName).getConstructor().newInstance().asInstanceOf[GraphBuilder[Any]]
  val routerReplicator = RaphtoryReplicator.apply("Router", 1, 1,graphBuilder)
  system.actorOf(Props(routerReplicator), s"Routers")

  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1,1)), s"PartitionManager")
  system.actorOf(Props[AnalysisManager], s"AnalysisManager")

  system.actorOf(Props(Class.forName(UpdaterName)), "Spout")



}
