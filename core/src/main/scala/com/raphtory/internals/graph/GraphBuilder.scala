package com.raphtory.internals.graph

import cats.Functor
import cats.effect.Async
import cats.effect.Ref
import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.google.protobuf.empty.Empty
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.defaultConf
import com.raphtory.protocol
import com.raphtory.protocol.GraphId
import com.raphtory.protocol.PartitionService
import fs2.Chunk

import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.ListHasAsScala

class GraphBuilderF[F[_], T](
    graphId: String,
    sourceId: Int,
    builder: GraphBuilder[T],
    partitions: Map[Int, PartitionService[F]],
    earliestSeen: Ref[F, Long],
    highestSeen: Ref[F, Long],
    sentUpdates: Ref[F, Long]
)(implicit F: Async[F]) {

  private val partitioner       = Partitioner(defaultConf)
  private val totalSourceErrors = TelemetryReporter.totalSourceErrors.labels(s"$sourceId", graphId) // TODO

  def buildGraphFromT(chunk: Chunk[T], globalIndex: Ref[F, Long]): F[Unit] =
    for {
      b <- F.delay(new mutable.ArrayBuffer[AnyRef](chunk.size))
      cb = UnsafeGraphCallback(partitions.size, sourceId, -1, graphId, b)
      _ <- globalIndex.getAndUpdate(_ + chunk.size).map { index =>
             // we reserved the chunk size in the global index
             cb.index = index
             chunk.foldLeft(cb) { (cb, t) =>
               builder(cb, t)
               cb.index += 1
               cb
             }
           }
      _ <- prepareGraphUpdates(cb)(b.toArray).sequence_
    } yield ()

  def flush: F[Unit] =
    F.parSequenceN(partitions.size)(partitions.values.map(_.flush(GraphId(graphId))).toVector).void

  private def prepareGraphUpdates(cb: Graph)(updates: Array[AnyRef]): Vector[F[Unit]] =
    if (partitions.size > 1)
      updates
        .flatMap {
          case update: EdgeAdd     => partitioner.getPartitionsForEdge(update.srcId, update.dstId).map((_, update))
          case update: GraphUpdate => Seq((cb.getPartitionForId(update.srcId), update))
        }
        .groupMap { case (partition, update) => partition } { case (partition, update) => update }
        .map {
          case (partition, updates) =>
            processUpdates(partition, updates)
        }
        .toVector
    else
      Vector(processUpdates(partitions.head._1, updates.collect { case gu: GraphUpdate => gu }))

  private def processUpdates(partition: Int, updates: Array[GraphUpdate]): F[Unit]    =
    if (updates.length > 0) {

      val minTime = updates.minBy(_.updateTime).updateTime
      val maxTime = updates.maxBy(_.updateTime).updateTime
      for {
        _ <- partitions(partition)
               .processUpdates(
                       protocol
                         .GraphAlterations(graphId, alterations = updates.map(protocol.GraphAlteration(_)).toSeq)
               )
        _ <- earliestSeen.update(Math.min(_, minTime))
        _ <- highestSeen.update(Math.max(_, maxTime))
        _ <- sentUpdates.update(_ + updates.length)
      } yield ()
    }
    else F.unit

  def getSentUpdates: F[Long]   = sentUpdates.get
  def earliestTimeSeen: F[Long] = earliestSeen.get
  def highestTimeSeen: F[Long]  = highestSeen.get
}

/** This class implements Graph interface by putting updates into a provided array buffer
  * so we can get updates out of it to be sent to the partitions
  */
case class UnsafeGraphCallback[F[_]: Functor](
    totalPartitions: Int,
    sourceID: Int,
    var index: Long,
    graphID: String,
    b: mutable.ArrayBuffer[AnyRef]
) extends Graph {
  override protected def handleGraphUpdate(update: GraphUpdate): Unit = b += update

}
