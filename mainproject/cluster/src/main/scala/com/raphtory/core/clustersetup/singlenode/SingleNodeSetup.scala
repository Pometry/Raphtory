package com.raphtory.core.clustersetup.singlenode

import akka.actor.Props
import com.raphtory.core.analysis.API.Analyser
import com.raphtory.core.analysis.Tasks.LiveTasks.BWindowedLiveAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.LiveAnalysisTask
import com.raphtory.core.analysis.Tasks.LiveTasks.WindowedLiveAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.BWindowedRangeAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.RangeAnalysisTask
import com.raphtory.core.analysis.Tasks.RangeTasks.WindowedRangeAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.BWindowedViewAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.ViewAnalysisTask
import com.raphtory.core.analysis.Tasks.ViewTasks.WindowedViewAnalysisTask
import com.raphtory.core.clustersetup.DocSvr
import com.raphtory.core.components.ClusterManagement.RaphtoryReplicator
import com.raphtory.core.components.ClusterManagement.SeedActor
import com.raphtory.core.components.ClusterManagement.WatchDog
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.language.postfixOps

// TODO migrate to object props
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
