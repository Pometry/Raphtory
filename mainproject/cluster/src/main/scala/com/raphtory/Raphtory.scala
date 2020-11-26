package com.raphtory

import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog, WatermarkManager}
import com.raphtory.core.components.Router.GraphBuilder
import com.raphtory.core.components.Spout.{DataSource, Spout}
import kamon.Kamon
import org.slf4j.LoggerFactory

object Raphtory {
  def apply[T](dataSource: DataSource[T],graphBuilder: GraphBuilder[T]) : Raphtory[T] =
    new Raphtory(dataSource, graphBuilder)
}

class Raphtory[T](dataSource: DataSource[T],graphBuilder: GraphBuilder[T]) {
  Kamon.init() //start tool logging

  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val system = ActorSystem("Citation-system")

  val partitionNumber = 1
  val minimumRouters  = 1
  system.actorOf(Props(new WatermarkManager(managerCount = 1)),"WatermarkManager")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")

  system.actorOf(Props(new Spout(dataSource)), "Spout")
  system.actorOf(Props(RaphtoryReplicator.apply("Router", 1, 1,graphBuilder)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1,1)), s"PartitionManager")

  system.actorOf(Props[AnalysisManager], s"AnalysisManager")
  AnalysisRestApi(system)
}
