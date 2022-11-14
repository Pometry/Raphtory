package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Resource
import cats.effect.kernel.Ref
import cats.syntax.all._
import com.raphtory.internals.graph.GraphBuilderF
import com.raphtory.protocol.WriterService
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
      writers: Map[Int, WriterService[F]]
  ): Resource[F, StreamSource[F, MessageType]] =
    builder
      .make(graphID, id, writers)
      .map(builder => new StreamSource[F, MessageType](id, spout.buildSpout(), builder))
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

}

//class SourceInstance[F[_], T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderInstance[F, T]) {
//
//  /** Logger instance for writing out log messages */
//  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
//  def sourceID: Int  = id
//
//  def pollInterval: FiniteDuration = 1.seconds
//
//  def hasRemainingUpdates: Boolean = spoutInstance.hasNext
//
//  def sendUpdates(index: Long, failOnError: Boolean): Unit = {
//    val element = spoutInstance.next()
//    builderInstance.sendUpdates(element, index)(failOnError)
//  }
//
//  def spoutReschedules(): Boolean = spoutInstance.spoutReschedules()
//
////  def executeReschedule(): Unit = spoutInstance.executeReschedule()
//
////  def close(): Unit = spoutInstance.close()
//
//  def sentMessages(): Long = builderInstance.getSentUpdates
//
//  def highestTimeSeen(): Long = builderInstance.highestTimeSeen
//}

class StreamSource[F[_], T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderF[F, T])(implicit
    F: Async[F]
) {

  def elements(counter: Counter.Child): fs2.Stream[F, Unit] =
    for {
      index <- fs2.Stream.eval(Ref.of[F, Long](1L))
      tuples = fs2.Stream.fromBlockingIterator[F](spoutInstance, 512)
      _     <- (tuples.chunks.head.evalMap(chunk =>
                   builderInstance.buildGraphFromT(chunk, index) *> F.delay(counter.inc(chunk.size))
                 ) ++ tuples.chunks.tail.parEvalMapUnordered(4)(chunk =>
                   builderInstance.buildGraphFromT(chunk, index) *> F.delay(counter.inc(chunk.size))
                 )).last
    } yield ()

  def sentMessages: F[Long] = builderInstance.getSentUpdates

  def highestTimeSeen(): F[Long]     = builderInstance.highestTimeSeen
  def spoutReschedules(): F[Boolean] = F.delay(spoutInstance.spoutReschedules())
  def pollInterval: FiniteDuration   = 1.seconds
  def sourceID: Int                  = id
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source =
    new ConcreteSource(spout, ClosureCleaner.clean(builder))

}
