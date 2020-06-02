package com.raphtory.core.components.PartitionManager.Workers

import akka.actor.Actor
import akka.actor.ActorLogging
import com.raphtory.core.model.communication.ReportIntake
import com.raphtory.core.model.communication.ReportSize
import com.raphtory.core.model.graphentities.Entity
import kamon.Kamon
//import kamon.metric.CounterMetric
//import kamon.metric.GaugeMetric
import kamon.metric.MeasurementUnit
import monix.execution.atomic.AtomicInt

import scala.collection.parallel.mutable.ParTrieMap

// TODO Re-add reportSizesVertex (removed during commit Log-Revamp)
class WriterLogger extends Actor with ActorLogging {

  val kLogging: Boolean = System.getenv().getOrDefault("PROMETHEUS", "true").trim().toBoolean

//  val bytesGauge: GaugeMetric        = Kamon.gauge("raphtory.heap.bytes", MeasurementUnit.information.bytes)
//  val instancesGauge: GaugeMetric    = Kamon.gauge("raphtory.heap.instances", MeasurementUnit.none)
//  val kGauge: GaugeMetric            = Kamon.gauge("raphtory.benchmarker")
//  val verticesGauge: GaugeMetric     = Kamon.gauge("raphtory.vertices")
//  val edgesGauge: GaugeMetric        = Kamon.gauge("raphtory.edges")
//  val mainMessages: GaugeMetric      = Kamon.gauge("raphtory.mainMessages")
//  val secondaryMessages: GaugeMetric = Kamon.gauge("raphtory.secondaryMessages")
//  val workerMessages: GaugeMetric    = Kamon.gauge("raphtory.workerMessages")

//  val kCounter: CounterMetric = Kamon.counter("raphtory.counters")

  override def preStart(): Unit =
    log.debug("WriterLogger is being started.")

  override def receive: Receive = {
 //   case req: ReportIntake => processReportIntakeRequest(req)
    case ReportSize(_)     =>
    // reportSizes(edgesGauge, EntityStorage.edges,partitionid)
    // reportSizes(verticesGauge, EntityStorage.vertices,partitionid)

  }

//  def reportSizes[T, U <: Entity](g: kamon.metric.GaugeMetric, map: ParTrieMap[T, U], id: Int): Unit =
//    try {
//      def getGauge(name: String) =
//        g.refine("actor" -> "PartitionManager", "replica" -> id.toString, "name" -> name)
//
//      getGauge("Total number of entities").set(map.size)
//      getGauge("Total number of previous states").set(getEntitiesPrevStates(map))
//
//    } catch {
//      case e: Exception =>
//        println(s"Caught exception in logging: $e")
//    }
//
//  def getEntitiesPrevStates[T, U <: Entity](m: ParTrieMap[T, U]): Int = {
//    var ret = AtomicInt(0)
//    m.foreach[Unit](e => ret.add(e._2.getHistorySize()))
//    ret.get
//  }
//
//  def processReportIntakeRequest(req: ReportIntake): Unit = {
//    log.debug("WriterLogger received [{}] request.", req)
//
//    req match {
//      case ReportIntake(messageCount, secondaryMessageCount, workerMessageCount, id, timeDifference) if kLogging =>
//        try {
//          kGauge
//            .refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString)
//            .set(messageCount / timeDifference)
//          mainMessages
//            .refine("actor" -> "PartitionManager", "name" -> "messageCount", "replica" -> id.toString)
//            .set(messageCount / timeDifference)
//          secondaryMessages
//            .refine("actor" -> "PartitionManager", "name" -> "secondaryMessageCount", "replica" -> id.toString)
//            .set(secondaryMessageCount / timeDifference)
//          workerMessages
//            .refine("actor" -> "PartitionManager", "name" -> "workerMessageCount", "replica" -> id.toString)
//            .set(workerMessageCount / timeDifference)
//        } catch {
//          case e: Exception => log.error("Failed to log report intake due to [{}].", e)
//        }
//    }
//  }
}
