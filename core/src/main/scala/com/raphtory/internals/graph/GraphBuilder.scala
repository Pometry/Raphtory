package com.raphtory.internals.graph

import cats.Functor
import cats.effect.Async
import cats.effect.Ref
import cats.effect.std.Dispatcher
import cats.effect.std.Queue
import cats.syntax.all._
import com.raphtory.api.input._
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol
import com.raphtory.protocol.Empty
import com.raphtory.protocol.WriterService

import scala.util.control.NonFatal

class GraphBuilderF[F[_], T](
    graphId: String,
    sourceId: Int,
    builder: GraphBuilder[T],
    writers: Map[Int, WriterService[F]],
    highestSeen: Ref[F, Long],
    sentUpdates: Ref[F, Long]
)(implicit F: Async[F]) {

  private val totalSourceErrors = TelemetryReporter.totalSourceErrors.labels(s"$sourceId", graphId)

  def buildGraphFromT(t: T, index: Long): F[Unit] =
    Dispatcher[F].use { d =>
      for {
        q <- Queue.unbounded[F, Option[GraphUpdate]]
        cb = UnsafeGraphCallback(writers.size, sourceId, index, graphId, d, q)
        _ <- F.delay(builder(cb, t)) *> q.offer(None)
        _ <- cb.updates
               .parEvalMapUnordered(8) { update =>
                 val partitionForTuple = cb.getPartitionForId(update.srcId)
                 (for {
                   _ <- (writers(partitionForTuple).processAlteration(protocol.GraphAlteration(update)))
                   _ <- highestSeen.update(Math.max(_, update.updateTime))
                   _ <- sentUpdates.update(_ + 1L)
                 } yield ()).handleError{case NonFatal(t) => }
               }
               .void
               .compile
               .drain
      } yield ()
    }
}

private[raphtory] class GraphBuilderInstance[F[_], T](
    graphId: String,
    sourceId: Int,
    builder: GraphBuilder[T],
    q: Queue[F, (GraphUpdate, Int)],
    dispatcher: Dispatcher[F],
//    unsafeGraphCallback: UnsafeGraphCallback[F],
    writers: Map[Int, WriterService[F]]
)(implicit F: Async[F])
        extends Serializable
        with Graph {

  override protected def sourceID: Int = sourceId

  override protected def graphID: String = graphId

  /** Logger instance for writing out log messages */
  var highestTimeSeen: Long           = Long.MinValue
  var internalIndex: Long             = -1L
  override def index: Long            = internalIndex
  private val partitionIDs            = writers.keySet
  private val internalTotalPartitions = writers.size
  def totalPartitions: Int            = internalTotalPartitions
  private var sentUpdates: Long       = 0
  private val totalSourceErrors       = TelemetryReporter.totalSourceErrors.labels(s"$sourceID", graphID)

  def getGraphID: String         = graphID
  def getSourceID: Int           = sourceID
  def parseTuple(tuple: T): Unit = builder(this, tuple)

  private[raphtory] def getSentUpdates: Long = sentUpdates

  /** Parses `tuple` and fetches list of updates for the graph This is used internally to retrieve updates. */
  private[raphtory] def sendUpdates(tuple: T, tupleIndex: Long)(failOnError: Boolean = true): Unit =
    try {
      logger.trace(s"Parsing tuple: $tuple with index $tupleIndex")
      internalIndex = tupleIndex
      parseTuple(tuple)

    }
    catch {
      case e: Exception =>
        if (failOnError) {
          e.printStackTrace()
          totalSourceErrors.inc()
          throw e
        }
        else {
          logger.warn(s"Failed to parse tuple.", e.getMessage)
          totalSourceErrors.inc()
          e.printStackTrace()
        }
    }

  override protected def handleGraphUpdate(update: GraphUpdate): Unit = {
    logger.trace(s"handling $update")
    sentUpdates += 1
    highestTimeSeen = highestTimeSeen max update.updateTime
    val partitionForTuple = getPartitionForId(update.srcId)
    if (partitionIDs contains partitionForTuple) {
      dispatcher.unsafeRunSync(q.offer((update, partitionForTuple)))
      logger.trace(s"$update sent")
    }
  }

  private def runAlteration(update: GraphUpdate, partitionForTuple: Int): F[Empty] =
    writers(partitionForTuple).processAlteration(protocol.GraphAlteration(update))

}

case class UnsafeGraphCallback[F[_]: Functor](
    totalPartitions: Int,
    sourceID: Int,
    index: Long,
    graphID: String,
    d: Dispatcher[F],
    q: Queue[F, Option[GraphUpdate]]
) extends Graph {
  override protected def handleGraphUpdate(update: GraphUpdate): Unit = d.unsafeRunSync(q.offer(Some(update)))

  def updates: fs2.Stream[F, GraphUpdate] = fs2.Stream.fromQueueNoneTerminated(q)

}
