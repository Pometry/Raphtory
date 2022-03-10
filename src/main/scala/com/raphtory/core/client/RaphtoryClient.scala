package com.raphtory.core.client

import com.raphtory.core.algorithm.RaphtoryGraphBuilder
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.time.DiscreteInterval
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

private[core] class RaphtoryClient(
    private val queryBuilder: QueryBuilder,
    private val conf: Config
) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  // Raphtory Client extends scheduler, queries return QueryProgressTracker, not threaded worker
  def pointQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      timestamp: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val raphtorizedQueryBuilder = queryBuilder
      .setEndTime(timestamp)
      .setWindows(windows map DiscreteInterval)
    val graph                   = new RaphtoryGraphBuilder(raphtorizedQueryBuilder)
    graph.execute(graphAlgorithm).writeTo(outputFormat)
  }

  def rangeQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val raphtorizedQueryBuilder = queryBuilder
      .setStartTime(start)
      .setEndTime(end)
      .setIncrement(DiscreteInterval(increment))
      .setWindows(windows map DiscreteInterval)
    val graph                   = new RaphtoryGraphBuilder(raphtorizedQueryBuilder)
    graph.execute(graphAlgorithm).writeTo(outputFormat)
  }

  def liveQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val raphtorizedQueryBuilder = queryBuilder
      .setIncrement(DiscreteInterval(increment))
      .setWindows(windows map DiscreteInterval)
    val graph                   = new RaphtoryGraphBuilder(raphtorizedQueryBuilder)
    graph.execute(graphAlgorithm).writeTo(outputFormat)
  }

  def getConfig(): Config = conf
}
