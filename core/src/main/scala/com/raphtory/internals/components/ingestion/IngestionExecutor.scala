package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.syntax.all._
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.EndPoint
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.BlockIngestion
import com.raphtory.internals.components.querymanager.NonBlocking
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

private[raphtory] class IngestionExecutor[F[_]: Async](
    graphID: String,
    source: Source,
    blocking: Boolean,
    sourceID: Int,
    conf: Config,
    topics: TopicRepository
) {
  private val logger: Logger                               = Logger(LoggerFactory.getLogger(this.getClass))
  private val failOnError                                  = conf.getBoolean("raphtory.builders.failOnError")
  private val writers: Map[Int, EndPoint[GraphAlteration]] = topics.graphUpdates(graphID).endPoint()
  private val queryManager                                 = topics.blockingIngestion(graphID).endPoint
  private val sourceInstance                               = source.buildSource(graphID, sourceID)
  private val totalTuplesProcessed                         = TelemetryReporter.totalTuplesProcessed.labels(s"$sourceID", graphID)

  private var index: Long = 0

  sourceInstance.setupStreamIngestion(writers)

  def release(): F[Unit] =
    for {
//      _ <- Async[F].delay(close()) -> Needed if this class extends FlushToFlight
      _ <- Async[F].delay(writers.values.foreach(_.close()))
      _ <- Async[F].delay(queryManager.close())
    } yield ()

  def run(): F[Unit] =
    for {
      _ <- Async[F].delay(logger.debug("Running ingestion executor"))
      _ <- iterativePolls
    } yield ()

  private def iterativePolls: F[Unit] =
    for {
      _ <- uniquePoll
      _ <- if (sourceInstance.spoutReschedules()) waitForNextPoll *> iterativePolls else Async[F].unit
    } yield ()

  private def waitForNextPoll: F[Unit] =
    for {
      _ <- Async[F].delay(logger.trace(s"Spout: Scheduling spout to poll again in ${sourceInstance.pollInterval}."))
      _ <- Async[F].sleep(sourceInstance.pollInterval)
    } yield ()

  private def uniquePoll: F[Unit] = Async[F].blocking(executePoll())

  private def executePoll(): Unit = {
    var iBlocked = false

    if (sourceInstance.hasRemainingUpdates)
      if (blocking) {
        queryManager.sendAsync(BlockIngestion(sourceID = sourceInstance.sourceID, graphID = graphID))
        iBlocked = true
      }
      else
        queryManager.sendAsync(NonBlocking(sourceID = sourceInstance.sourceID, graphID = graphID))

    while (sourceInstance.hasRemainingUpdates) {
//      latestMsgTimeToFlushToFlight = System.currentTimeMillis() -> Needed if this class extends FlushToFlight
      totalTuplesProcessed.inc()
      index = index + 1
      sourceInstance.sendUpdates(index, failOnError)
    }
    // TODO
    // here it already knows that it has consumed all the gu from the source
    // after pedro changes it will also know that writers have seen all the gus
    // now IE can let queryService know that all the GUs are processed
    if (blocking && iBlocked) {
      val id  = sourceInstance.sourceID
      val msg =
        UnblockIngestion(id, graphID, sourceInstance.sentMessages(), sourceInstance.highestTimeSeen(), force = false)
      queryManager sendAsync msg
    }
  }
}

object IngestionExecutor {

  def apply[F[_]: Async](
      graphID: String,
      source: Source,
      blocking: Boolean,
      sourceID: Int,
      config: Config,
      topics: TopicRepository
  ): Resource[F, IngestionExecutor[F]] = {
    val createExecutor =
      Async[F].delay(new IngestionExecutor(graphID, source, blocking, sourceID, config, topics))
    Resource.make(createExecutor)(executor => executor.release())
  }
}
