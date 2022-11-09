package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.kernel.Ref
import cats.effect.std.Dispatcher
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.api.input.SourceInstance
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
import com.raphtory.protocol.WriterService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import fs2.Stream
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
  private val logger: Logger       = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError          = conf.getBoolean("raphtory.builders.failOnError")
  private val queryManager         = topics.blockingIngestion(graphID).endPoint
  private val totalTuplesProcessed = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

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
      _    <- uniquePoll
      wait <- source.spoutReschedules()
      _    <- if (wait) waitForNextPoll else F.unit
    } yield ()

  private def waitForNextPoll: F[Unit] =
    for {
      _ <- F.delay(logger.trace(s"Spout: Scheduling spout to poll again in ${source.pollInterval}."))
      _ <- F.sleep(source.pollInterval)
    } yield ()

  private def uniquePoll: F[Unit] = executePoll()

  private def executePoll(): F[Unit] = {
    val s = for {
      iBlocked <- fs2.Stream.eval(Ref.of(false))
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
      _        <- source.elements // process elements here
      _        <- fs2.Stream.eval {
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
    } yield ()

//    if (source.hasRemainingUpdates)
//      if (blocking) {
//        queryManager.sendAsync(BlockIngestion(sourceID = source.sourceID, graphID = graphID))
//        iBlocked = true
//      }
//      else
//        queryManager.sendAsync(NonBlocking(sourceID = source.sourceID, graphID = graphID))

//    while (source.hasRemainingUpdates) {
//      latestMsgTimeToFlushToFlight = System.currentTimeMillis() -> Needed if this class extends FlushToFlight
    totalTuplesProcessed.inc()
//    index = index + 1
//      source.sendUpdates(index, failOnError)
//      val iterator: Stream.PartiallyAppliedFromIterator[Nothing] = fs2.Stream.fromIterator
//    }
//    if (blocking && iBlocked) {
//      val id  = source.sourceID
//      val msg =
//        UnblockIngestion(id, graphID, source.sentMessages(), source.highestTimeSeen(), force = false)
//      queryManager sendAsync msg
//    }
    s.void.compile.drain
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
    def createExecutor(sourceInstance: SourceInstance[F, _]) =
      Async[F].delay(new IngestionExecutor(graphID, sourceInstance, blocking, sourceID, config, registry.topics))
    for {
      writers        <- registry.writers(graphID)
      sourceInstance <- source.make(graphID, sourceID, writers)
      executor       <- Resource.make(createExecutor(sourceInstance))(executor => executor.release())
    } yield executor
  }
}
