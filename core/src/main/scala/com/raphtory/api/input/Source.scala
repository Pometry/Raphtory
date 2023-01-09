package com.raphtory.api.input

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphBuilderF
import com.twitter.chill.ClosureCleaner
import io.prometheus.client.Counter

import scala.concurrent.duration._

trait Source {
  def getDynamicClasses: List[Class[_]] = List()
  def makeStream[F[_]: Async]: F[fs2.Stream[F, Seq[GraphUpdate]]]
}

abstract class SpoutBuilderSource[T] extends Source {
  protected def spout: Spout[T]
  protected def builder: GraphBuilder[T]
  override def getDynamicClasses = List(builder.getClass)

  override def makeStream[F[_]: Async]: F[fs2.Stream[F, Seq[GraphUpdate]]] =
    for {
      spoutInstance   <- Async[F].delay(spout.buildSpout())
      builderInstance <- builder.make
      stream          <- Async[F].pure(for {
                           index   <- fs2.Stream.eval(Ref.of[F, Long](1L))
                           tuples   = fs2.Stream.fromBlockingIterator[F](spoutInstance, 512)
                           // TODO: process chunks in parallel and increment the index consistently
                           updates <- tuples.chunks.evalMap(chunk => builderInstance.parseUpdates(chunk, index))
                         } yield updates)
    } yield stream
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source = {
    val (spout_, builder_) = (spout, builder) // to avoid conflicts with method names
    new SpoutBuilderSource[T] {
      override protected def spout: Spout[T]          = spout_
      override protected def builder: GraphBuilder[T] = ClosureCleaner.clean(builder_)
    }
  }
}
