package com.raphtory.core.actors.partitionmanager.Writer.Helpers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication.ReportIntake
import com.raphtory.core.model.graphentities.Entity
import com.raphtory.core.storage.EntityStorage
import kamon.Kamon
import kamon.metric.GaugeMetric
import monix.eval.Task

import scala.collection.parallel.mutable.ParTrieMap

class LoggingSlave extends RaphtoryActor{

  val verticesGauge         : GaugeMetric = Kamon.gauge("raphtory.vertices")
  val edgesGauge            : GaugeMetric = Kamon.gauge("raphtory.edges")

  val kLogging              : Boolean = System.getenv().getOrDefault("PROMETHEUS", "true").trim().toBoolean // should the state of the vertex/edge map be output to Kamon/Prometheus
  val stdoutLog             : Boolean = System.getenv().getOrDefault("STDOUT_LOG", "true").trim().toBoolean // A slower logging for the state of vertices/edges maps to Stdout

  override def receive:Receive = {
    case ReportIntake(mainMessages,secondaryMessages,partitionId) => reportIntake(mainMessages,secondaryMessages,partitionId)

  }

  def getEntitiesPrevStates[T,U <: Entity](m : ParTrieMap[T, U]) : Int = {
    var ret = new AtomicInteger(0)
    m.foreach[Unit](e => {
      ret.getAndAdd(e._2.getPreviousStateSize())
    })
    ret.get
  }

  def reportSizes[T, U <: Entity](g : kamon.metric.GaugeMetric, map : ParTrieMap[T, U],id:Int) : Unit = {
    def getGauge(name : String) = {g.refine("actor" -> "PartitionManager", "replica" -> id.toString, "name" -> name)}
    getGauge("Total number of entities").set(map.size)
    getGauge("Total number of previous states").set(getEntitiesPrevStates(map))
    // getGauge("Total number of properties") TODO
    // getGauge("Number of props previous history") TODO
  }

  def reportIntake(messageCount:Int, secondaryMessageCount:Int, id:Int) : Unit = {
    //println(s"TrieMaps size: ${EntityStorage.edges.size}\t${EntityStorage.vertices.size} | " +
    //      s"TreeMaps size: ${getEntitiesPrevStates(EntityStorage.edges)}\t${getEntitiesPrevStates(EntityStorage.vertices)}")
    // Kamon monitoring
    if (kLogging) {
      kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString).set(messageCount)
      kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount", "replica" -> id.toString).set(secondaryMessageCount)
      reportSizes(edgesGauge, EntityStorage.edges,id)
      reportSizes(verticesGauge, EntityStorage.vertices,id)
    }
    // Heap benchmarking
    //profile()
    // Reset counters
  }
  //def reportStdout() : Unit = {
  //  if (stdoutLog)
  //
  // }

}
