package com.raphtory.internals.graph

import cats.Functor
import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol
import com.raphtory.protocol.PartitionService
import fs2.Chunk

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GraphBuilderF[F[_], T](
    graphId: String,
    sourceId: Int,
    builder: GraphBuilder[T],
    partitions: Map[Int, PartitionService[F]],
    earliestSeen: Ref[F, Long],
    highestSeen: Ref[F, Long],
    sentUpdates: Ref[F, Long]
)(implicit F: Async[F]) {

  private val partitioner       = Partitioner()
  private val totalSourceErrors = TelemetryReporter.totalSourceErrors.labels(s"$sourceId", graphId) // TODO

  def buildGraphFromT(chunk: Chunk[T], index: Ref[F, Long]): F[Unit] =
    for {
      b <- F.delay(new mutable.ArrayBuffer[GraphUpdate](chunk.size))
      cb = UnsafeGraphCallback(partitions.size, sourceId, -1, graphId, b)
      _ <- index.getAndUpdate(_ + chunk.size).map { index =>
             chunk.foldLeft(index) { (i, t) =>
               builder(cb.copy(index = i), t)
               i + 1
             }
           }

      _ <- prepareGraphUpdates(cb)(b.toSeq).sequence_

    } yield ()

  private def prepareGraphUpdates(cb: Graph)(updates: Seq[GraphUpdate]): Vector[F[Unit]] =
    updates
      .flatMap {
        case update: EdgeAdd => partitioner.getPartitionsForEdge(update.srcId, update.dstId).toSeq.map((_, update))
        case update          => Seq((cb.getPartitionForId(update.srcId), update))
      }
      .groupMap { case (partition, update) => partition } { case (partition, update) => update }
      .map {
        case (partition, updates) =>
          val minTime = updates.minBy(_.updateTime).updateTime
          val maxTime = updates.maxBy(_.updateTime).updateTime
          for {
            _ <- partitions(partition)
                   .processUpdates(
                           protocol.GraphAlterations(graphId, alterations = updates.map(protocol.GraphAlteration(_)))
                   )
            _ <- earliestSeen.update(Math.min(_, minTime))
            _ <- highestSeen.update(Math.max(_, maxTime))
            _ <- sentUpdates.update(_ + updates.size)
          } yield ()
      }
      .toVector

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
    index: Long,
    graphID: String,
    b: mutable.ArrayBuffer[GraphUpdate]
) extends Graph {
  override protected def handleGraphUpdate(update: GraphUpdate): Unit = b += update

}
