import java.lang.management.ManagementFactory

import akka.actor.{ActorSystem, Props}
import ch.qos.logback.classic.Level
import com.raphtory.core.analysis.{AnalysisManager, AnalysisRestApi}
import com.raphtory.core.components.ClusterManagement.{RaphtoryReplicator, WatchDog, WatermarkManager}
import kamon.Kamon
import org.slf4j.LoggerFactory

object TemporalTriangleCountExample extends App{
  Kamon.init() //start tool logging

  val runtimeMxBean = ManagementFactory.getRuntimeMXBean
  val arguments     = runtimeMxBean.getInputArguments

  println(s"Current java options: $arguments")

  val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[ch.qos.logback.classic.Logger]
  root.setLevel(Level.ERROR)
  val system = ActorSystem("Citation-system")

  val partitionNumber = 1
  val minimumRouters  = 1
  system.actorOf(Props(new WatermarkManager(managerCount = 1)),"WatermarkManager")
  system.actorOf(Props(new WatchDog(partitionNumber, minimumRouters)), "WatchDog")


  //var SpoutName ="com.raphtory.examples.test.actors.TriangleTestSpout"
  //var SpoutName = "com.raphtory.examples.gab.actors.GabExampleSpout"
  var SpoutName = "com.raphtory.spouts.FirehoseSpout"
  system.actorOf(Props(Class.forName(SpoutName)), "Spout")

  //var routerClassName = "com.raphtory.examples.test.actors.TriangleTestRouter"
  var routerClassName = "com.raphtory.examples.gab.actors.GabUserGraphRouter"
  system.actorOf(Props(RaphtoryReplicator("Router", 1, routerClassName)), s"Routers")

  system.actorOf(Props(RaphtoryReplicator("Partition Manager", 1)), s"PartitionManager")

  system.actorOf(Props[AnalysisManager], s"AnalysisManager")
  AnalysisRestApi(system)
}
