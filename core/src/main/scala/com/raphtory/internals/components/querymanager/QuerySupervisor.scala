package com.raphtory.internals.components.querymanager

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
class QuerySupervisor private (
    graphID: GraphID,
    topics: TopicRepository,
    config: Config,
    private val blockingSources: mutable.Set[SourceID],
    private val blockedQueries: mutable.Set[Query],
    private val currentQueries: mutable.Map[String, QueryHandler]
) {
  private val logger           = Logger(LoggerFactory.getLogger(this.getClass))
  private var earliestTimeSeen = -1L
  private var latestTimeSeen   = -1L

  def startBlockingIngestion(sourceID: Int): Unit = {
    if (!blockingSources.contains(sourceID))
      blockingSources.add(sourceID)
    logger.info(s"Source '$sourceID' is blocking analysis for Graph '$graphID'")
  }

  def endBlockingIngestion(sourceID: Int, _earliestTimeSeen: Long, _latestTimeSeen: Long): Unit = {
    earliestTimeSeen = _earliestTimeSeen
    latestTimeSeen = _latestTimeSeen

    blockingSources.remove(sourceID)

    blockedQueries.filterInPlace { query =>
      if (!isBlockIngesting(query.blockedBy)) {
        spawnQueryHandler(query)
        false
      }
      else true
    }

    logger.info(
            s"Source '$sourceID' is unblocking analysis for Graph '$graphID' with earliest time seen as $earliestTimeSeen and latest time seen as $latestTimeSeen"
    )
  }

  def submitQuery(query: Query): Unit = {
    logger.debug(s"Handling query: $query")

    query.blockedBy.foreach(blockingSources.add)

    if (isBlockIngesting(query.blockedBy)) {
      blockedQueries.addOne(query)
      logger.info(s"Query '${query.name}' currently blocked, waiting for ingestion to complete.")
    }
    else spawnQueryHandler(query)
  }

  def endQuery(jobID: JobID): Unit = {
    currentQueries.get(jobID) match {
      case Some(queryhandler) =>
        queryhandler.stop()
        currentQueries.remove(jobID)
      case None               =>
    }
    blockedQueries.filterInPlace(q => q.name != jobID)
  }

  private def spawnQueryHandler(query: Query): Unit = {
    val qh = QueryHandler(this, new Scheduler, query, config, topics, earliestTimeSeen, latestTimeSeen)
    currentQueries.addOne(query.name, qh)
    TelemetryReporter.totalQueriesSpawned.labels(graphID).inc()
  }

  private def isBlockIngesting(blockedBy: Array[Long]): Boolean =
    blockedBy.forall(blockingSources.contains)
}

object QuerySupervisor {
  type GraphID  = String
  type SourceID = Long
  type JobID    = String

  def apply(graphID: GraphID, topics: TopicRepository, config: Config): QuerySupervisor =
    new QuerySupervisor(
            graphID,
            topics,
            config,
            mutable.Set[Long](),
            mutable.Set[Query](),
            mutable.Map[String, QueryHandler]()
    )
}
