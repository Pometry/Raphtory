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
                           updates <- tuples.chunks.evalMap(chunk => builderInstance.parseUpdates(chunk, index))
                         } yield updates)
    } yield stream
}

class StreamSource[F[_], T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderF[F, T])(implicit
    F: Async[F]
) {

  def elements(counter: Counter.Child): fs2.Stream[F, collection.Seq[GraphUpdate]] =
    for {
      index   <- fs2.Stream.eval(Ref.of[F, Long](1L))
      tuples   = fs2.Stream.fromBlockingIterator[F](spoutInstance, 512)
      updates <- tuples.chunks.parEvalMapUnordered(4)(chunk =>
                   builderInstance.parseUpdates(chunk, index) <* F.delay(counter.inc(chunk.size))
                 )
    } yield updates

//  def sentMessages: F[Long]          = builderInstance.getSentUpdates
//  def earliestTimeSeen(): F[Long] = builderInstance.earliestTimeSeen
//  def highestTimeSeen(): F[Long]  = builderInstance.highestTimeSeen
//  def spoutReschedules(): F[Boolean] = F.delay(spoutInstance.spoutReschedules())
//  def pollInterval: FiniteDuration   = 1.seconds
//  def sourceID: Int                  = id
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
