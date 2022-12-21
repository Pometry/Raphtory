package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Ref
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.api.input.StreamSource
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.management.Partitioner
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol
import com.raphtory.protocol._
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.prometheus.client.Counter
import org.slf4j.LoggerFactory

private[raphtory] class IngestionExecutor[F[_], T](
    graphID: String,
    queryService: QueryService[F],
    partitions: Map[Int, PartitionService[F]],
    stream: fs2.Stream[F, Seq[GraphUpdate]],
    sourceID: Int,
    conf: Config
)(implicit F: Async[F]) {
  import IngestionExecutor.SourceStats
  private val logger: Logger                      = Logger(LoggerFactory.getLogger(this.getClass))
  private val totalTuplesProcessed: Counter.Child = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

  private val partitioner = Partitioner(conf)

  def run(): F[Unit] =
    for {
      _           <- F.delay(logger.debug("Running ingestion executor"))
      globalStats <- Ref.of(SourceStats(Long.MaxValue, Long.MinValue, 0L))
      _           <- stream
                       .parEvalMapUnordered(4)(updates => // Here we are setting the parallelism
                         for {
                           _          <- sendUpdates(updates)
                           localStats <- SourceStats.fromUpdates(updates)
                           _          <- globalStats.update(stats => stats.add(localStats))
                         } yield ()
                       )
                       .compile
                       .drain
      stats       <- globalStats.get
      _           <- queryService.endIngestion(EndIngestion(graphID, sourceID, stats.earliestTime, stats.highestSeen))
    } yield totalTuplesProcessed.inc() // TODO: wtf is this?

  private def sendUpdates(updates: Seq[GraphUpdate]): F[Unit] =
    for {
      groupedUpdates <-
        F.delay(
                updates
                  .flatMap {
                    case update: EdgeAdd =>
                      partitioner.getPartitionsForEdge(update.srcId, update.dstId).toSeq.map((_, update))
                    case update          => Seq((partitioner.getPartitionForId(update.srcId), update))
                  }
                  .groupMap { case (partition, update) => partition } { case (partition, update) => update }
        )
      _              <- F.parSequenceN(partitions.size)(groupedUpdates.toSeq.map {
                          case (partition, updates) =>
                            partitions(partition)
                              .processUpdates(protocol.GraphAlterations(graphID, updates.map(protocol.GraphAlteration(_))))
                        })
    } yield ()
}

object IngestionExecutor {

  case class SourceStats(earliestTime: Long, highestSeen: Long, sentUpdates: Long) {

    def add(other: SourceStats): SourceStats =
      SourceStats(
              Math.min(this.earliestTime, other.earliestTime),
              Math.max(this.highestSeen, other.highestSeen),
              this.sentUpdates + other.sentUpdates
      )
  }

  object SourceStats {

    def fromUpdates[F[_]: Async](graphUpdates: Seq[GraphUpdate]): F[SourceStats] =
      Async[F].delay {
        val minTime = graphUpdates.minBy(_.updateTime).updateTime
        val maxTime = graphUpdates.maxBy(_.updateTime).updateTime
        SourceStats(minTime, maxTime, graphUpdates.size)
      }
  }

  def apply[F[_]: Async](
      graphID: String,
      queryService: QueryService[F],
      source: Source,
      sourceID: Int,
      config: Config,
      partitions: Map[Int, PartitionService[F]]
  ): F[IngestionExecutor[F, _]] =
    for {
      _        <- queryService.startIngestion(StartIngestion(sourceID, graphID))
      stream   <- source.makeStream
      executor <- Async[F].delay(new IngestionExecutor(graphID, queryService, partitions, stream, sourceID, config))
    } yield executor
}
