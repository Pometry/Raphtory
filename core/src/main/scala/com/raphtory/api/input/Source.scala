package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Resource
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
  ): Resource[F, StreamSource[F, MessageType]] =
    builder
      .make(graphID, id, partitions)
      .map(builder => new StreamSource[F, MessageType](id, spout.buildSpout(), builder))
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

}

class StreamSource[F[_], T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderF[F, T])(implicit
    F: Async[F]
) {

  def elements(counter: Counter.Child): F[Unit] = {
    val s = for {
      index <- fs2.Stream.eval(Ref.of[F, Long](1L))
      tuples = fs2.Stream.fromBlockingIterator[F](spoutInstance, 512)
      _     <- tuples.chunks.parEvalMapUnordered(4)(chunk =>
                 builderInstance.buildGraphFromT(chunk, index) *> F.delay(counter.inc(chunk.size))
               )
    } yield ()

    s.void.compile.drain
  }

  def sentMessages: F[Long]          = builderInstance.getSentUpdates
  def earliestTimeSeen(): F[Long]    = builderInstance.earliestTimeSeen
  def highestTimeSeen(): F[Long]     = builderInstance.highestTimeSeen
  def spoutReschedules(): F[Boolean] = F.delay(spoutInstance.spoutReschedules())
  def pollInterval: FiniteDuration   = 1.seconds
  def sourceID: Int                  = id
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source =
    new ConcreteSource(spout, ClosureCleaner.clean(builder))

}
