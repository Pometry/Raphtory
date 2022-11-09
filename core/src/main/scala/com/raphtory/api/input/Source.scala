package com.raphtory.api.input

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Sync
import cats.syntax.all._
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphBuilderF
import com.raphtory.internals.graph.GraphBuilderInstance
import com.raphtory.internals.graph.UnsafeGraphCallback
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.protocol.WriterService
import com.twitter.chill.ClosureCleaner
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

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
  ): Resource[F, SourceInstance[F, MessageType]] =
    builder
      .make(graphID, id, writers)
      .map(builder => new SourceInstance[F, MessageType](id, spout.buildSpout(), builder))
}

class ConcreteSource[T](override val spout: Spout[T], override val builder: GraphBuilder[T]) extends Source {
  override type MessageType = T

}

class SourceInstance[F[_], T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderInstance[F, T]) {

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  def sourceID: Int  = id

  def pollInterval: FiniteDuration = 1.seconds

  def hasRemainingUpdates: Boolean = spoutInstance.hasNext

  def sendUpdates(index: Long, failOnError: Boolean): Unit = {
    val element = spoutInstance.next()
    builderInstance.sendUpdates(element, index)(failOnError)
  }

  def spoutReschedules(): Boolean = spoutInstance.spoutReschedules()

//  def executeReschedule(): Unit = spoutInstance.executeReschedule()

//  def close(): Unit = spoutInstance.close()

  def sentMessages(): Long = builderInstance.getSentUpdates

  def highestTimeSeen(): Long = builderInstance.highestTimeSeen
}

class StreamSource[F[_], T](id: Int, spoutInstance: SpoutInstance[T], builderInstance: GraphBuilderF[F, T])(implicit
    F: Async[F]
) {

  def elements =
    for {
      index <- fs2.Stream.eval(Ref.of[F, Long](0L))
//      d     <- fs2.Stream.resource(Dispatcher[F])
//      q     <- fs2.Stream.eval(Queue.unbounded[F, GraphUpdate])

      _ <- fs2.Stream
             .fromBlockingIterator[F](spoutInstance, 1024)
             .parEvalMapUnordered(4) { t =>
               for {
                 i <- index.getAndUpdate(_ + 1)
                 _ <- builderInstance.buildGraphFromT(t, i)
               } yield ()
             }
    } yield ()

  def sentMessages: F[Long] = F.delay(builderInstance.getSentUpdates)

  def highestTimeSeen(): F[Long]     = F.delay(builderInstance.highestTimeSeen)
  def spoutReschedules(): F[Boolean] = F.delay(spoutInstance.spoutReschedules())
  def pollInterval: FiniteDuration   = 1.seconds
  def sourceID: Int                  = id
}

object Source {

  def apply[T](spout: Spout[T], builder: GraphBuilder[T]): Source =
    new ConcreteSource(spout, ClosureCleaner.clean(builder))

}
