package com.raphtory.tests

import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog, WatermarkManager}
import kamon.Kamon
import org.slf4j.LoggerFactory

object CitationExample extends App{
  Kamon.init() //start tool logging

  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val system = ActorSystem("Citation-system")

  val partitionNumber = 1
  val minimumRouters  = 1
  system.actorOf(Props(new WatermarkManager(managerCount = 1)),"WatermarkManager")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")


  //var SpoutName = "com.raphtory.spouts.FileSpout"
  var SpoutName = "com.raphtory.examples.gab.actors.GabExampleSpout"
  system.actorOf(Props(Class.forName(SpoutName)), "Spout")

  var routerClassName = "com.raphtory.examples.gab.actors.GabUserGraphRouter"
  //var routerClassName = "com.raphtory.examples.citationNetwork.CitationRouter"
  system.actorOf(Props(RaphtoryReplicator("Router", 1, routerClassName)), s"Routers")

  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1)), s"PartitionManager")

  system.actorOf(Props[AnalysisManager], s"AnalysisManager")
  AnalysisRestApi(system)
}
