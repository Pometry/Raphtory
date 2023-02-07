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

  /** This returns F[Stream] instead of just Stream because that gives you a chance to execute
    * some things in the context of the F to prepare the source (for instance checking that a file actually exists)
    * so the exceptions bubble up to the client context reporting the problem
    */
  def makeStream[F[_]: Async](globalIndex: Ref[F, Long]): F[fs2.Stream[F, Seq[GraphUpdate]]]
}

abstract class SpoutBuilderSource[T] extends Source {
  protected def spout: Spout[T]
  protected def builder: GraphBuilder[T]
  override def getDynamicClasses = List(builder.getClass)

  override def makeStream[F[_]: Async](globalIndex: Ref[F, Long]): F[fs2.Stream[F, Seq[GraphUpdate]]] =
    for {
      builderInstance <- builder.make
      tuples           = spout.asStream
      stream           = tuples.chunks.evalMap(chunk => builderInstance.parseUpdates(chunk, globalIndex))
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
