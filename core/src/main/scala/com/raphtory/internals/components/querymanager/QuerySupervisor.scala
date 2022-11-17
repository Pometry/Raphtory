package com.raphtory.internals.components.querymanager

import cats.syntax.all._
import cats.effect._
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.QuerySupervisor._
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.telemetry.TelemetryReporter
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import javax.annotation.concurrent.NotThreadSafe
import scala.collection.mutable

@NotThreadSafe
class QuerySupervisor[F[_]] private (
    graphID: GraphID,
    topics: TopicRepository,
    config: Config,
    private val blockingSources: mutable.Set[SourceID],
    private val blockedQueries: mutable.Set[Query],
    private val currentQueries: mutable.Set[JobID]
)(implicit F: Async[F]) {
  private val logger       = Logger(LoggerFactory.getLogger(this.getClass))
  private var earliestTime = -1L
  private var latestTime   = -1L

  def startBlockingIngestion(sourceID: Int): F[Unit] =
    F.delay {
      if (!blockingSources.contains(sourceID))
        blockingSources.add(sourceID)
      logger.info(s"Source '$sourceID' is blocking analysis for Graph '$graphID'")
    }

  private def isBlockIngesting(blockedBy: Array[Long]): F[Boolean] =
    F.delay(blockedBy.forall(blockingSources.contains))

  private def spawnQueryHandler(query: Query): F[Unit] =
    QueryHandler(this, new Scheduler, query, config, topics, earliestTime, latestTime) *>
      F.delay {
        currentQueries.addOne(query.name)
        TelemetryReporter.totalQueriesSpawned.labels(graphID).inc()
      }

  def endBlockingIngestion(sourceID: Int, _earliestTime: Long, _latestTime: Long): F[Unit] =
    for {
      _        <- F.delay {
                    earliestTime = _earliestTime
                    latestTime = _latestTime
                  }
      blockedQ <- blockedQueries.toList.filterA { query =>
                    for {
                      blockingIngestion <- isBlockIngesting(query.blockedBy)
                      _                 <- if (blockingIngestion) spawnQueryHandler(query) else F.unit
                    } yield blockingIngestion
                  }
      _        <- F.blocking {
                    blockedQ.foreach(blockedQueries.remove)
                    blockingSources.remove(sourceID)
                    logger.info(
                            s"Source '$sourceID' is unblocking analysis for Graph '$graphID' with earliest time seen as $earliestTime and latest time seen as $latestTime"
                    )
                  }
    } yield ()

  def submitQuery(query: Query): F[Unit] =
    for {
      _                 <- F.delay {
                             logger.info(s"Handling query: $query")
                             query.blockedBy.foreach(blockingSources.add)
                           }
      blockingIngestion <- isBlockIngesting(query.blockedBy)
      _                 <- if (blockingIngestion)
                             F.delay {
                               blockedQueries.addOne(query)
                               logger.info(s"Query '${query.name}' currently blocked, waiting for ingestion to complete.")
                             }
                           else spawnQueryHandler(query)
    } yield ()

  def endQuery(jobID: JobID): F[Unit] =
    F.delay {
      currentQueries.remove(jobID)
      blockedQueries.filterInPlace(q => q.name != jobID)
    }
}

object QuerySupervisor {
  type GraphID  = String
  type SourceID = Long
  type JobID    = String

  def apply[F[_]: Async](graphID: GraphID, topics: TopicRepository, config: Config): Resource[F, QuerySupervisor[F]] =
    Resource.make {
      Async[F].delay(
              new QuerySupervisor(
                      graphID,
                      topics,
                      config,
                      mutable.Set[SourceID](),
                      mutable.Set[Query](),
                      mutable.Set[JobID]()
              )
      )
    }(_ => Async[F].unit)
}
