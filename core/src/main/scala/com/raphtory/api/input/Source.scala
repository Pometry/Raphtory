package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Clock
import cats.effect.kernel.Ref
import cats.syntax.all._
import com.raphtory.internals.graph.GraphBuilderF
import com.raphtory.protocol.PartitionService
import com.twitter.chill.ClosureCleaner
import io.prometheus.client.Counter
import scala.concurrent.duration._

trait Source {
  type MessageType
  def spout: Spout[MessageType]
  def builder: GraphBuilder[MessageType]

  def getBuilderClass: Class[_] = builder.getClass

  def make[F[_]: Async](
      graphID: String,
      id: Int,
      partitions: Map[Int, PartitionService[F]]
  ): F[StreamSource[F, MessageType]] =
    builder
      .make(graphID, id, partitions)
      .map(builder => new StreamSource[F, MessageType](id, spout.asStream, builder))
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

}

class StreamSource[F[_], T](id: Int, tuples: fs2.Stream[F, T], builderInstance: GraphBuilderF[F, T])(implicit
    F: Async[F]
) {

  def processTuples(counter: Counter.Child): F[Unit] = {
    val s = for {
      index <- fs2.Stream.eval(Ref.of[F, Long](1L))
      start <- fs2.Stream.eval(Clock[F].monotonic)
      _     <- tuples.chunks.evalMap { chunk =>
                 builderInstance.buildGraphFromT(chunk, index) *> F.delay(counter.inc(chunk.size))
               }.last
      _     <- fs2.Stream.eval(builderInstance.flush)
      end   <- fs2.Stream.eval(Clock[F].monotonic)
      _     <- fs2.Stream.eval(F.delay(println(s"INNER INGESTION TOOK ${(end - start).toSeconds}s ")))
    } yield ()

    s.compile.drain
  }

  def sentMessages: F[Long]          = builderInstance.getSentUpdates
  def earliestTimeSeen(): F[Long]    = builderInstance.earliestTimeSeen
  def highestTimeSeen(): F[Long]     = builderInstance.highestTimeSeen
  def spoutReschedules(): F[Boolean] = F.delay(false)
  def pollInterval: FiniteDuration   = 1.seconds
  def sourceID: Int                  = id
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source =
    new ConcreteSource(spout, ClosureCleaner.clean(builder))

}
