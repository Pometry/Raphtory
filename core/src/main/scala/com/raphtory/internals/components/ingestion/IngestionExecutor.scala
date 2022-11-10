package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Ref
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.raphtory.protocol.BlockIngestion
import com.raphtory.protocol.QueryService
import com.raphtory.protocol.UnblockIngestion
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

private[raphtory] class IngestionExecutor[F[_]: Async](
    graphID: String,
    queryService: QueryService[F],
    source: Source,
    sourceID: Int,
    conf: Config,
    topics: TopicRepository
) {
  private val logger: Logger                               = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError                                  = conf.getBoolean("raphtory.builders.failOnError")
  private val writers: Map[Int, EndPoint[GraphAlteration]] = topics.graphUpdates(graphID).endPoint()
  private val sourceInstance                               = source.buildSource(graphID, sourceID)
  private val totalTuplesProcessed                         = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

  private var index: Long = 0

  sourceInstance.setupStreamIngestion(writers)

  def release(): F[Unit] =
    for {
//      _ <- Async[F].delay(close()) -> Needed if this class extends FlushToFlight
      _ <- Async[F].delay(writers.values.foreach(_.close()))
    } yield ()

  def run(): F[Unit] =
    for {
      _ <- Async[F].delay(logger.debug("Running ingestion executor"))
      _ <- iterativePolls
    } yield ()

  private def iterativePolls: F[Unit] =
    for {
      _ <- executePoll()
      _ <- if (sourceInstance.spoutReschedules()) waitForNextPoll *> iterativePolls else Async[F].unit
    } yield ()

  private def waitForNextPoll: F[Unit] =
    for {
      _ <- Async[F].delay(logger.trace(s"Spout: Scheduling spout to poll again in ${sourceInstance.pollInterval}."))
      _ <- Async[F].sleep(sourceInstance.pollInterval)
    } yield ()

  private def executePoll(): F[Unit] = {
    def sendUpdatesR(): F[Unit] =
      for {
        _ <- Async[F].blocking {
               totalTuplesProcessed.inc()
               index = index + 1
               sourceInstance.sendUpdates(index, failOnError)
             }
        _ <- if (sourceInstance.hasRemainingUpdates) sendUpdatesR() else Async[F].unit
      } yield ()

    for {
      isBlocked <- Ref.of(false)
      _         <- if (sourceInstance.hasRemainingUpdates)
                     queryService.blockIngestion(
                             BlockIngestion(sourceID = sourceInstance.sourceID, graphID = graphID)
                     ) *> isBlocked.set(true) *> sendUpdatesR()
                   else Async[F].unit
      blocked   <- isBlocked.get
      _         <- if (blocked)
                     queryService.unblockIngestion(
                             UnblockIngestion(graphID, sourceInstance.sourceID, 0, sourceInstance.highestTimeSeen())
                     )
                   else Async[F].unit
    } yield ()
  }
}

object IngestionExecutor {

  def apply[F[_]: Async](
      graphID: String,
      queryService: QueryService[F],
      source: Source,
      sourceID: Int,
      config: Config,
      topics: TopicRepository
  ): Resource[F, IngestionExecutor[F]] = {
    val createExecutor =
      Async[F].delay(new IngestionExecutor(graphID, queryService, source, sourceID, config, topics))
    Resource.make(createExecutor)(executor => executor.release())
  }
}
