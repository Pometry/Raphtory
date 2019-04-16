package com.raphtory.core.actors.partitionmanager.Writer.Helpers

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.Actor
import com.raphtory.core.actors.RaphtoryActor
import com.raphtory.core.model.communication.{ReportIntake, ReportSize}
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
    case ReportSize(partitionid) => {reportSizes(edgesGauge, EntityStorage.edges,partitionid);reportSizes(verticesGauge, EntityStorage.vertices,partitionid)}

  }

  def getEntitiesPrevStates[T,U <: Entity](m : ParTrieMap[T, U]) : Int = {
    var ret = 0
    m.foreach[Unit](e => {
      ret += e._2.getHistorySize()
    })
    ret
  }

  def reportSizes[T, U <: Entity](g : kamon.metric.GaugeMetric, map : ParTrieMap[T, U],id:Int) : Unit = {
    try {
      def getGauge(name: String) = {
        g.refine("actor" -> "PartitionManager", "replica" -> id.toString, "name" -> name)
      }

      getGauge("Total number of entities").set(map.size)
      getGauge("Total number of previous states").set(getEntitiesPrevStates(map))
      // getGauge("Total number of properties") TODO
      // getGauge("Number of props previous history") TODO
    }catch {
      case e:Exception =>{
        println(s"Caught exception in logging: $e")
      }
    }
  }

  def reportIntake(messageCount:Int, secondaryMessageCount:Int, id:Int) : Unit = {
    try {
      // Kamon monitoring
      if (kLogging) {
        //println(s"id= $id message count  $messageCount")
        kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString).set(messageCount)
        kGauge.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount", "replica" -> id.toString).set(secondaryMessageCount)
      }
    } catch {
      case e: Exception => {
        println(s"Caught exception in logging: $e")
      }
    }
  }


}
