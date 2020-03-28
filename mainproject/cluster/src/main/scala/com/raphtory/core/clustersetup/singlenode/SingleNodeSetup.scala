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
  //"redis-server --daemonize yes" ! //start redis running on manager partition
  //Process("cassandra").lineStream //run cassandara in background on manager
  //Thread.sleep(5000)
  //RaphtoryDBWrite.createDB()
  system.actorOf(Props(new SeedActor(this)), "cluster")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")
  system.actorOf(Props(RaphtoryReplicator("Router", 1, routerClassName)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1)), s"PartitionManager")
  system.actorOf(Props(Class.forName(UpdaterName)), "UpdateGen")

  val jobID    = sys.env.getOrElse("JOBID", "Default").toString
  val analyser = Class.forName(LAMName).newInstance().asInstanceOf[Analyser]

  Thread.sleep(30000)

  sys.env.getOrElse("LAMTYPE", "Live").toString match {
    case "Live" => // live graph
      sys.env.getOrElse("WINDOWTYPE", "false") match {
        case "false" =>
          system.actorOf(Props(new LiveAnalysisManager(jobID, analyser)), s"LiveAnalysisManager_$LAMName")
        case "true" =>
          system.actorOf(Props(new WindowedLiveAnalysisManager(jobID, analyser)), s"LiveAnalysisManager_$LAMName")
        case "batched" =>
          system.actorOf(Props(new BWindowedLiveAnalysisManager(jobID, analyser)), s"LiveAnalysisManager_$LAMName")
      }
    case "View" => //view of the graph
      val time = sys.env.getOrElse("TIMESTAMP", "0").toLong
      sys.env.getOrElse("WINDOWTYPE", "false") match {
        case "false" =>
          system.actorOf(Props(new ViewAnalysisManager(jobID, analyser, time)), s"ViewAnalysisManager_$LAMName")
        case "true" =>
          val window = sys.env.getOrElse("WINDOW", "0").toLong
          system.actorOf(
                  Props(new WindowedViewAnalysisManager(jobID, analyser, time, window)),
                  s"ViewAnalysisManager_$LAMName"
          )
        case "batched" =>
          val windowset = sys.env.getOrElse("WINDOWSET", "0").split(",").map(f => f.toLong)
          system.actorOf(
                  Props(new BWindowedViewAnalysisManager(jobID, analyser, time, windowset)),
                  s"ViewAnalysisManager_$LAMName"
          )
      }
    case "Range" => // windowed range query through history
      val start = sys.env.getOrElse("START", "0").toLong
      val end   = sys.env.getOrElse("END", "0").toLong
      val jump  = sys.env.getOrElse("JUMP", "0").toLong
      sys.env.getOrElse("WINDOWTYPE", "false") match {
        case "false" =>
          system.actorOf(
                  Props(new RangeAnalysisManager(jobID, analyser, start, end, jump)),
                  s"RangeAnalysisManager_$LAMName"
          )
        case "true" =>
          val window = sys.env.getOrElse("WINDOW", "0").toLong
          system.actorOf(
                  Props(new WindowedRangeAnalysisManager(jobID, analyser, start, end, jump, window)),
                  s"RangeAnalysisManager_$LAMName"
          )
        case "batched" =>
          val windowset = sys.env.getOrElse("WINDOWSET", "0").split(",").map(f => f.toLong)
          system.actorOf(
                  Props(new BWindowedRangeAnalysisManager(jobID, analyser, start, end, jump, windowset)),
                  s"RangeAnalysisManager_$LAMName"
          )
      }
  }
}
