package com.raphtory.core.clustersetup.singlenode

import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Managers.LiveManagers.BWindowedLiveAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.LiveAnalysisManager
import com.raphtory.core.analysis.Managers.LiveManagers.WindowedLiveAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.BWindowedRangeAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.RangeAnalysisManager
import com.raphtory.core.analysis.Managers.RangeManagers.WindowedRangeAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.BWindowedViewAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.ViewAnalysisManager
import com.raphtory.core.analysis.Managers.ViewManagers.WindowedViewAnalysisManager
import com.raphtory.core.clustersetup.DocSvr
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator
import com.raphtory.core.components.ClusterManagement.SeedActor
import com.raphtory.core.components.ClusterManagement.WatchDog
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

case class SingleNodeSetup(
    seedLoc: String,
    routerClassName: String,
    UpdaterName: String,
    LAMName: String,
    partitionNumber: Int,
    minimumRouters: Int
) extends DocSvr {
  val conf: Config    = ConfigFactory.load()
  implicit val system = initialiseActorSystem(List(seedLoc))

  system.actorOf(Props(new SeedActor(this)), "cluster")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router", 1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")



  Thread.sleep(30000)


}
