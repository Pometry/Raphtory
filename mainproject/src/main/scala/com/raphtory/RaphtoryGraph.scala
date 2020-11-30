package com.raphtory

import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog, WatermarkManager}
import com.raphtory.core.components.Router.GraphBuilder
import com.raphtory.core.components.Spout.{Spout, SpoutAgent}
import kamon.Kamon
import org.slf4j.LoggerFactory

object RaphtoryGraph {
  def apply[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) : RaphtoryGraph[T] =
    new RaphtoryGraph(spout, graphBuilder)

  def apply[T](spoutPath: String, graphBuilderPath: String) : RaphtoryGraph[T] ={
    val spout = Class.forName(spoutPath).getConstructor().newInstance().asInstanceOf[Spout[T]]
    val graphBuilder = Class.forName(graphBuilderPath).getConstructor().newInstance().asInstanceOf[GraphBuilder[T]]
    new RaphtoryGraph(spout, graphBuilder)
  }

}

class RaphtoryGraph[T](spout: Spout[T], graphBuilder: GraphBuilder[T]) {
  Kamon.init() //start tool logging

//  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  //root.setLevel(Level.ERROR)
  val system = ActorSystem("Citation-system")

  val partitionNumber = 1
  val minimumRouters  = 1
  system.actorOf(Props(new WatermarkManager(partitionNumber)),"WatermarkManager")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")

  system.actorOf(Props(new SpoutAgent(spout)), "Spout")
  system.actorOf(Props(RaphtoryReplicator.apply("Router", partitionNumber, minimumRouters,graphBuilder)), s"Routers")
  system.actorOf(Props(RaphtoryReplicator("Partition Manager", partitionNumber,minimumRouters)), s"PartitionManager")

  system.actorOf(Props[AnalysisManager], s"AnalysisManager")
  AnalysisRestApi(system)
}
