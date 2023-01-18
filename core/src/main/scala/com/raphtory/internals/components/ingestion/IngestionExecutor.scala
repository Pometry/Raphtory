package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Clock
import cats.effect.Ref
import cats.effect.kernel.Resource.ExitCase.Succeeded
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.internals.graph.GraphAlteration.EdgeAdd
import com.raphtory.internals.graph.GraphAlteration.GraphUpdate
import com.raphtory.internals.graph.GraphAlteration.VertexAdd
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
  private val vertexAddCounter                    = TelemetryReporter.vertexAddCounter.labels(s"$sourceID", graphID)
  private val edgeAddCounter                      = TelemetryReporter.edgeAddCounter.labels(s"$sourceID", graphID)

  private val partitioner = Partitioner(conf)

  def run(): F[Unit] =
    for {
      _           <- F.delay(logger.debug("Running ingestion executor"))
      globalStats <- Ref.of(SourceStats(Long.MaxValue, Long.MinValue, 0L))
      start       <- Clock[F].monotonic
      _           <- poke
      _           <- stream
                       .evalMap(updates => // Here we are setting the parallelism
                         for {
                           _ <- sendUpdates(updates)
                           _ <- globalStats.update(stats => stats.add(updates))
                           _ <- F.delay {
                                  vertexAddCounter.inc(updates.count(_.isInstanceOf[VertexAdd]))
                                  edgeAddCounter.inc(updates.count(_.isInstanceOf[EdgeAdd]))
                                  totalTuplesProcessed.inc(updates.size)
                                }
                         } yield ()
                       )
                       .onFinalizeCase {
                         case Succeeded =>
                           for {
                             _   <- flush
                             end <- Clock[F].monotonic
                             _   <- F.delay(logger.debug(s"INNER INGESTION TOOK ${(end - start).toSeconds}s "))
                             _   <- notifyQueryService(globalStats)
                           } yield ()
                         case _         =>
                           for {
                             _ <- F.delay(logger.error(s"Ingestion failed. Unblocking query service"))
                             _ <- notifyQueryService(globalStats)
                           } yield ()
                       }
                       .compile
                       .drain
    } yield ()

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
      _              <- groupedUpdates.toSeq.map {
                          case (partition, updates) =>
                            partitions(partition)
                              .processUpdates(protocol.GraphAlterations(graphID, updates))
                        }.sequence_
    } yield ()

  private def poke: F[Unit] =
    F.parSequenceN(partitions.size)(partitions.values.map(_.poke(GraphId(graphID))).toVector).void

  private def flush: F[Unit] =
    F.parSequenceN(partitions.size)(partitions.values.map(_.flush(GraphId(graphID))).toVector).void

  private def notifyQueryService(globalStats: Ref[F, SourceStats]): F[Unit] =
    for {
      stats <- globalStats.get
      _     <- queryService.endIngestion(EndIngestion(graphID, sourceID, stats.earliestTime, stats.highestSeen))
    } yield ()
}

object IngestionExecutor {

  case class SourceStats(earliestTime: Long, highestSeen: Long, sentUpdates: Long) {

    def add(graphUpdates: Seq[GraphUpdate]): SourceStats = {
      val minTime = graphUpdates.minBy(_.updateTime).updateTime
      val maxTime = graphUpdates.maxBy(_.updateTime).updateTime
      SourceStats(Math.min(earliestTime, minTime), Math.max(highestSeen, maxTime), sentUpdates + graphUpdates.size)
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
