package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.api.input.StreamSource
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol.BlockIngestion
import com.raphtory.protocol.PartitionService
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
    conf: Config
)(implicit F: Async[F]) {
  private val logger: Logger                      = Logger(LoggerFactory.getLogger(this.getClass))
  private val totalTuplesProcessed: Counter.Child = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

  def run(): F[Unit] =
    for {
      _                <- F.delay(logger.debug("Running ingestion executor"))
      _                <- queryService.blockIngestion(
                                  BlockIngestion(source.sourceID, graphID)
                          )
      _                <- source.elements(totalTuplesProcessed) // process elements here
      earliestTimeSeen <- source.earliestTimeSeen()
      highestTimeSeen  <- source.highestTimeSeen()
      _                <- queryService.unblockIngestion(
                                  UnblockIngestion(graphID, source.sourceID, earliestTimeSeen, highestTimeSeen)
                          )
    } yield totalTuplesProcessed.inc()
}

object IngestionExecutor {

  def apply[F[_]: Async](
      graphID: String,
      queryService: QueryService[F],
      source: Source,
      sourceID: Int,
      config: Config,
      partitions: Map[Int, PartitionService[F]]
  ): F[IngestionExecutor[F, _]] =
    for {
      streamSource <- source.make(graphID, sourceID, partitions)
      executor     <- Async[F].delay(new IngestionExecutor(graphID, queryService, streamSource, sourceID, config))
    } yield executor
}
