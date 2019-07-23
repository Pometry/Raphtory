package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.Actor
import com.raphtory.core.model.communication.{ReportIntake, ReportSize}
import com.raphtory.core.model.graphentities.Entity
import com.raphtory.core.storage.EntityStorage
import kamon.Kamon
import kamon.metric.{GaugeMetric, MeasurementUnit}

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

class WriterLogger extends Actor{

  val bytesGauge     = Kamon.gauge("raphtory.heap.bytes", MeasurementUnit.information.bytes)
  val instancesGauge = Kamon.gauge("raphtory.heap.instances", MeasurementUnit.none)
  val kGauge         = Kamon.gauge("raphtory.benchmarker")
  val kCounter       = Kamon.counter("raphtory.counters")

  val verticesGauge         : GaugeMetric = Kamon.gauge("raphtory.vertices")
  val edgesGauge            : GaugeMetric = Kamon.gauge("raphtory.edges")

  val kLogging              : Boolean = System.getenv().getOrDefault("PROMETHEUS", "true").trim().toBoolean // should the state of the vertex/edge map be output to Kamon/Prometheus
  val stdoutLog             : Boolean = System.getenv().getOrDefault("STDOUT_LOG", "true").trim().toBoolean // A slower logging for the state of vertices/edges maps to Stdout
  val mainMessages         = Kamon.gauge("raphtory.mainMessages")
  val secondaryMessages    = Kamon.gauge("raphtory.secondaryMessages")
  val workerMessages       = Kamon.gauge("raphtory.workerMessages")

  override def receive:Receive = {
    case ReportIntake(mainMessages,secondaryMessages,workerMessages,partitionId,timeDifference) => reportIntake(mainMessages,secondaryMessages,workerMessages,partitionId,timeDifference)
    case ReportSize(partitionid) => {reportSizes(edgesGauge, EntityStorage.edges,partitionid);reportSizes(verticesGauge, EntityStorage.vertices,partitionid)}

  }

  def getEntitiesPrevStates[T,U <: Entity](m :ParTrieMap[T, U]) : Int = {
    var ret = 0
    m.foreach[Unit](e => {
      ret += e._2.getHistorySize()
    })
    ret
  }

  def reportSizes[T, U <: Entity](g : kamon.metric.GaugeMetric, map :ParTrieMap[T, U], id:Int) : Unit = {
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

  def reportIntake(messageCount:Int, secondaryMessageCount:Int, workerMessageCount:Int, id:Int,timeDifference:Long) : Unit = {
    try {
      // Kamon monitoring
      if (kLogging) {
        kGauge.refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString).set(messageCount/timeDifference)
        mainMessages.refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString).set(messageCount/timeDifference)
        secondaryMessages.refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount", "replica" -> id.toString).set(secondaryMessageCount/timeDifference)
        workerMessages.refine("actor" -> "PartitionManager", "name" -> "workerMessageCount", "replica" -> id.toString).set(workerMessageCount/timeDifference)
      }
    } catch {
      case e: Exception => {
        println(s"Caught exception in logging: $e")
      }
    }
  }


}
