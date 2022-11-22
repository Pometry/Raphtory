package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.api.input.StreamSource
import com.raphtory.internals.FlushToFlight
import com.raphtory.internals.communication.CanonicalTopic
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.components.querymanager.BlockIngestion
import com.raphtory.internals.components.querymanager.NonBlocking
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import fs2.Stream
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private[raphtory] class IngestionExecutor[F[_], T](
    graphID: String,
    source: StreamSource[F, T],
    blocking: Boolean,
    sourceID: Int,
    conf: Config,
    topics: TopicRepository
)(implicit F: Async[F]) {
  private val logger: Logger                      = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError                         = conf.getBoolean("raphtory.builders.failOnError")
  private val queryManager                        = topics.blockingIngestion(graphID).endPoint
  private val totalTuplesProcessed: Counter.Child = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

  private var index: Long = 0

  def release(): F[Unit] =
    for {
//      _ <- Async[F].delay(close()) -> Needed if this class extends FlushToFlight
      _ <- F.delay(queryManager.close())
    } yield ()

  def run(): F[Unit] =
    for {
      _ <- F.delay(logger.debug("Running ingestion executor"))
      _ <- iterativePolls
    } yield ()

  private def iterativePolls: F[Unit] =
    for {
      _    <- executePoll
      wait <- source.spoutReschedules()
      _    <- if (wait) waitForNextPoll else F.unit
    } yield ()

  private def waitForNextPoll: F[Unit] =
    for {
      _ <- F.delay(logger.trace(s"Spout: Scheduling spout to poll again in ${source.pollInterval}."))
      _ <- F.sleep(source.pollInterval)
    } yield ()

  private def executePoll: F[Unit] = {

    val s = for {
      iBlocked <- fs2.Stream.eval(Ref.of(blocking))
      _        <- if (blocking)
                    fs2.Stream.eval(
                            F.delay(
                                    queryManager.sendAsync(BlockIngestion(sourceID = source.sourceID, graphID = graphID))
                            ) *> iBlocked.set(true)
                    )
                  else
                    fs2.Stream.eval(
                            F.delay(
                                    queryManager.sendAsync(NonBlocking(sourceID = source.sourceID, graphID = graphID))
                            ) *> iBlocked.set(false)
                    )
      _        <- source.elements(totalTuplesProcessed).last.onComplete(finaliseIngestion(iBlocked)) // process elements here
    } yield ()

    totalTuplesProcessed.inc()
    s.void.compile.drain
  }

  private def finaliseIngestion(iBlocked: Ref[F, Boolean]) =
    fs2.Stream.eval {
      for {
        blocked         <- iBlocked.get
        sentMessages    <- source.sentMessages
        highestTimeSeen <- source.highestTimeSeen()
        _               <- if (blocked && blocking)
                             F.delay {
                               val id  = source.sourceID
                               val msg =
                                 UnblockIngestion(id, graphID, sentMessages, highestTimeSeen, force = false)
                               queryManager sendAsync msg
                             }
                           else F.unit
      } yield ()
    }
}

object IngestionExecutor {

  def apply[F[_]: Async](
      graphID: String,
      source: Source,
      blocking: Boolean,
      sourceID: Int,
      config: Config,
      registry: ServiceRegistry[F]
  ): Resource[F, IngestionExecutor[F, _]] = {
    def createExecutor(streamSource: StreamSource[F, _]) =
      Async[F].delay(new IngestionExecutor(graphID, streamSource, blocking, sourceID, config, registry.topics))
    for {
      partitions   <- registry.partitions
      streamSource <- source.make(graphID, sourceID, partitions)
      executor     <- Resource.make(createExecutor(streamSource))(executor => executor.release())
    } yield executor
  }
}
