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
import cats.Traverse
import com.raphtory.protocol
import fs2.Stream

class QuerySupervisor[F[_]] private (
    graphID: GraphID,
    topics: TopicRepository,
    config: Config,
    private val blockingSources: Ref[F, Map[SourceID, ISBLOCKING]],
    private val blockedQueries: Ref[F, Set[Query]],
    private val currentQueries: Ref[F, Set[JobID]]
)(implicit F: Async[F]) {
  private val logger       = Logger(LoggerFactory.getLogger(this.getClass))
  private var earliestTime = -1L
  private var latestTime   = -1L

  def startBlockingIngestion(sourceID: SourceID): F[Unit] =
    blockingSources.update { sources =>
      if (sources contains sourceID) sources
      else {
        val r = (sourceID, true)
        sources + r
      }
    } >> F.delay(logger.info(s"Source '$sourceID' is blocking analysis for Graph '$graphID'"))

  private def isBlockIngesting(blockedBy: Array[Long]): F[Boolean] =
    for {
      bs <- blockingSources.get
      res = blockedBy.forall(id => bs.contains(id) && bs(id))
    } yield res

  private def spawnQueryHandler(query: Query): F[Unit] =
    QueryHandler(this, new Scheduler, query, config, topics, earliestTime, latestTime) *>
      currentQueries.update(_ + query.name) >>
      F.delay(TelemetryReporter.totalQueriesSpawned.labels(graphID).inc())

  def endBlockingIngestion(sourceID: Int, _earliestTime: Long, _latestTime: Long): F[Unit] =
    for {
      _        <- F.delay {
                    earliestTime = _earliestTime
                    latestTime = _latestTime
                  }
      bs       <- blockedQueries.get
      blockedQ <- Traverse[List].traverse(bs.toList) { query =>
                    for {
                      blockingIngestion <- isBlockIngesting(query.blockedBy)
                      res               <- if (blockingIngestion)
                                             spawnQueryHandler(query) >>
                                               F.pure(Option(query))
                                           else
                                             F.pure(Option.empty[Query])
                    } yield res
                  }
      _        <- blockedQueries.update(_ diff blockedQ.filter(_.isDefined).map(_.get).toSet)
      _        <- blockingSources.update { sources =>
                    val r: (SourceID, ISBLOCKING) = (sourceID, false)
                    sources + r
                  }
      _        <- F.blocking {
                    logger.info(
                            s"Source '$sourceID' is unblocking analysis for Graph '$graphID' with earliest time seen as $earliestTime and latest time seen as $latestTime"
                    )
                  }
    } yield ()

  def submitQuery(query: Query): F[Stream[F, protocol.QueryManagement]] =
    for {
      _                 <- F.delay(logger.debug(s"Handling query: $query"))
      _                 <- blockingSources.update { sources =>
                             sources ++ (query.blockedBy.toList diff sources.keys.toList).map(_ -> true).toMap
                           }
      blockingIngestion <- isBlockIngesting(query.blockedBy)
      _                 <- blockedQueries.update { queries =>
                             if (blockingIngestion) queries + query else queries
                           }
      _                 <- if (blockingIngestion)
                             F.delay(logger.info(s"Query '${query.name}' currently blocked, waiting for ingestion to complete."))
                           else spawnQueryHandler(query)
    } yield ()

  def endQuery(jobID: JobID): F[Unit] =
    currentQueries.update(_ - jobID) *>
      blockedQueries.update(_.filter(q => q.name != jobID))
}

object QuerySupervisor {
  type GraphID    = String
  type SourceID   = Long
  type JobID      = String
  type ISBLOCKING = Boolean

  def apply[F[_]: Async](graphID: GraphID, topics: TopicRepository, config: Config): Resource[F, QuerySupervisor[F]] =
    for {
      blockingSources <- Resource.eval(Ref.of(Map[SourceID, ISBLOCKING]()))
      blockedQueries  <- Resource.eval(Ref.of(Set[Query]()))
      currentQueries  <- Resource.eval(Ref.of(Set[JobID]()))
      service         <- Resource.make {
                           Async[F].delay(
                                   new QuerySupervisor(
                                           graphID,
                                           topics,
                                           config,
                                           blockingSources,
                                           blockedQueries,
                                           currentQueries
                                   )
                           )
                         }(_ => Async[F].unit)
    } yield service
}
