package com.raphtory.internals.graph

import cats.Functor
import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
import fs2.Chunk

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class GraphBuilderF[F[_], T](builder: GraphBuilder[T])(implicit F: Async[F]) {
  // private val totalSourceErrors = TelemetryReporter.totalSourceErrors.labels(s"$sourceId", graphId) // FIXME: update this variable

  def parseUpdates(chunk: Chunk[T], index: Ref[F, Long]): F[Seq[GraphUpdate]] =
    for {
      b <- F.delay(new mutable.ArrayBuffer[GraphUpdate](chunk.size))
      cb = UnsafeGraphCallback(-1, b)
      _ <- index.getAndUpdate(_ + chunk.size).map { index =>
             chunk.foldLeft(index) { (i, t) =>
               builder(cb.copy(index = i), t) // TODO: try to find a better way of updating the index
               i + 1
             }
           }
    } yield b.toSeq
}

/** This class implements Graph interface by putting updates into a provided array buffer
  * so we can get updates out of it to be sent to the partitions
  */
case class UnsafeGraphCallback[F[_]: Functor](
    index: Long,
    b: mutable.ArrayBuffer[GraphUpdate]
) extends Graph {
  override protected def handleGraphUpdate(update: GraphUpdate): Unit = b += update
}
