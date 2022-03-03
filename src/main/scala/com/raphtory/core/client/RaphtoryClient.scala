package com.raphtory.core.client

import com.raphtory.core.algorithm.GenericGraphPerspective
import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.PulsarController
import com.raphtory.core.time.DiscreteInterval
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

private[core] class RaphtoryClient(
    private val deploymentID: String,
    private val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val pulsarController: PulsarController
) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  val queryBuilder: QueryBuilder =
    new QueryBuilder(deploymentID, conf, componentFactory, scheduler, pulsarController)

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
    val graph                   = new GenericGraphPerspective(raphtorizedQueryBuilder)
    graphAlgorithm.run(graph).writeTo(outputFormat)
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
    val graph                   = new GenericGraphPerspective(raphtorizedQueryBuilder)
    graphAlgorithm.run(graph).writeTo(outputFormat)
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
    val graph                   = new GenericGraphPerspective(raphtorizedQueryBuilder)
    graphAlgorithm.run(graph).writeTo(outputFormat)
  }

  def getConfig(): Config = conf
}
