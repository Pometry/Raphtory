package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.kernel.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.api.input.StreamSource
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol.BlockIngestion
import com.raphtory.protocol.QueryService
import com.raphtory.protocol.UnblockIngestion
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory

private[raphtory] class IngestionExecutor[F[_], T](
    graphID: String,
    queryService: QueryService[F],
    source: StreamSource[F, T],
    sourceID: Int,
    conf: Config,
    topics: TopicRepository
)(implicit F: Async[F]) {
  private val logger: Logger                      = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError                         = conf.getBoolean("raphtory.builders.failOnError")
  private val totalTuplesProcessed: Counter.Child = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

  def release(): F[Unit] =
    for {
//      _ <- Async[F].delay(close()) -> Needed if this class extends FlushToFlight
      _ <- F.unit
    } yield ()

  def run(): F[Unit] =
    for {
      _ <- F.delay(logger.debug("Running ingestion executor"))
      _ <- executePoll()
    } yield ()

  private def executePoll(): F[Unit] =
    for {
      isBlocked <- Ref.of(false)
      _         <- queryService.blockIngestion(
                           BlockIngestion(source.sourceID, graphID)
                   ) *> isBlocked.set(true)

      _         <- source.elements(totalTuplesProcessed) // process elements here
      _         <- finaliseIngestion(isBlocked)
    } yield totalTuplesProcessed.inc()

  private def finaliseIngestion(isBlocked: Ref[F, Boolean]) =
    for {
      blocked          <- isBlocked.get
      earliestTimeSeen <- source.earliestTimeSeen()
      highestTimeSeen  <- source.highestTimeSeen()
      _                <- if (blocked)
                            queryService.unblockIngestion(
                                    UnblockIngestion(graphID, source.sourceID, earliestTimeSeen, highestTimeSeen)
                            )
                          else F.unit
    } yield ()
}

object IngestionExecutor {

  def apply[F[_]: Async](
      graphID: String,
      queryService: QueryService[F],
      source: Source,
      sourceID: Int,
      config: Config,
      registry: ServiceRegistry[F]
  ): Resource[F, IngestionExecutor[F, _]] = {
    def createExecutor(streamSource: StreamSource[F, _]) =
      Async[F].delay(new IngestionExecutor(graphID, queryService, streamSource, sourceID, config, registry.topics))
    for {
      partitions   <- registry.partitions
      streamSource <- source.make(graphID, sourceID, partitions)
      executor     <- Resource.make(createExecutor(streamSource))(executor => executor.release())
    } yield executor
  }
}
