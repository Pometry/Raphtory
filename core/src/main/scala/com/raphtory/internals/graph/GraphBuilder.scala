package com.raphtory.internals.graph

import cats.Functor
import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
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
    highestSeen: Ref[F, Long],
    sentUpdates: Ref[F, Long]
)(implicit F: Async[F]) {

  private val totalSourceErrors = TelemetryReporter.totalSourceErrors.labels(s"$sourceId", graphId) // TODO

  def buildGraphFromT(t: Chunk[T], index: Ref[F, Long]): F[Unit] =
    timed(
            s"BATCH PROCESSING OF ${t.size}",
            for {
              b <- F.delay(new mutable.ArrayBuffer[GraphUpdate](t.size))
              cb = UnsafeGraphCallback(partitions.size, sourceId, -1, graphId, b)
              _ <- timed(
                           "HANDOVER into cats-effect",
                           index.getAndUpdate(_ + t.size).map { index =>
                             t.foldLeft(index) { (i, t) =>
                               builder(cb.copy(index = i), t)
                               i + 1
                             }
                           }
                   )
              _ <- timed(
                           "SENDING TO WRITERS",
                           processGraphUpdates(b, cb)
                   )
            } yield ()
    )

  private def processGraphUpdates(b: ArrayBuffer[GraphUpdate], cb: UnsafeGraphCallback[F]) =
//    F.parSequenceN(4)(prepareGraphUpdates(cb)(b.toSeq))
    prepareGraphUpdates(cb)(b.toSeq).sequence_

  def timed[A](msg: String, f: F[A]): F[A] =
    F.timed(f).flatMap {
      case (t, a) =>
//        println(s"TIME ${t.toMillis}ms for $msg")
        F.pure(a)
    }

  private def prepareGraphUpdates(cb: Graph)(updates: Seq[GraphUpdate]): Vector[F[Unit]] =
    updates
      .groupBy(update => cb.getPartitionForId(update.srcId))
      .map {
        case (partition, updates) =>
          val maxTime = updates.maxBy(_.updateTime).updateTime
          for {
            _ <- partitions(partition)
                   .processAlterations(
                           protocol.GraphAlterations(graphId, alterations = updates.map(protocol.GraphAlteration(_)))
                   )
            _ <- highestSeen.update(Math.max(_, maxTime))
            _ <- sentUpdates.update(_ + updates.size)
          } yield ()
      }
      .toVector

  def getSentUpdates: F[Long]  = sentUpdates.get
  def highestTimeSeen: F[Long] = highestSeen.get
}

case class UnsafeGraphCallback[F[_]: Functor](
    totalPartitions: Int,
    sourceID: Int,
    index: Long,
    graphID: String,
    b: mutable.ArrayBuffer[GraphUpdate]
) extends Graph {
  override protected def handleGraphUpdate(update: GraphUpdate): Unit = b += update

}
