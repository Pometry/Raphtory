package com.raphtory.client

import com.raphtory.algorithms.api.Alignment
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.TemporalGraph
import com.raphtory.components.Component
import com.raphtory.components.querymanager.Query
import com.raphtory.components.querytracker.QueryProgressTracker
import com.raphtory.config.ComponentFactory
import com.raphtory.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Exposes query APIs for point, range and live queries
  *
  *  `RaphtoryClient` should not be created directly. To create a `RaphtoryClient` use
  *  `Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.
  *
  * Usage while querying:
  *
  * @example
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.algorithm.api.OutputFormat
  * import com.raphtory.components.graphbuilder.GraphBuilder
  * import com.raphtory.components.spout.Spout
  *
  * val graph = Raphtory.createGraph[T](spout: Spout[T], graphBuilder: GraphBuilder[T])
  * val testDir  =  "/tmp/raphtoryTest"
  * val outputFormat: FileOutputFormat  =  FileOutputFormat(testDir)
  *
  * // Run Point Query:
  * graph.pointQuery(EdgeList(), outputFormat, timestamp: Long)
  *
  * // Run Range Query and wait until done:
  * val queryProgressTracker = graph.rangeQuery(graphAlgorithm = EdgeList(), outputFormat = outputFormat, start = 1, end = 32674, increment = 10000, windows = List(500, 1000, 10000))
  * queryProgressTracker.waitForJob()
  *
  *  @see
  *  [[com.raphtory.algorithms.api.GraphAlgorithm]], [[com.raphtory.components.querytracker.QueryProgressTracker]],
  *  [[com.raphtory.output.FileOutputFormat]], [[com.raphtory.output.PulsarOutputFormat]], [[com.raphtory.deployment.Raphtory]]
  */
private[raphtory] class RaphtoryClient(
    private val querySender: QuerySender,
    private val conf: Config
) {

  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  //
  /** Point query for querying at a specific timestamp, returns [`QueryProgressTracker`](com.raphtory.components.querytracker.QueryProgressTracker)
    * Raphtory Client extends scheduler, queries return QueryProgressTracker, not threaded worker
    * @param graphAlgorithm Graph algorithm to use for query
    * @param outputFormat Type of output format for storing results
    * @param timestamp Timestamp of query
    * @param windows If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`
    * @return a tracker tied to the job status
    *  */
  def pointQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      timestamp: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobName = graphAlgorithm.getClass.getCanonicalName.split("\\.").last
    new TemporalGraph(Query(), querySender, conf)
      .at(timestamp)
      .window(windows, Alignment.END)
      .execute(graphAlgorithm)
      .writeTo(outputFormat, jobName)
  }

  /**  Point query for querying at a specific timestamp, returns [`QueryProgressTracker`](com.raphtory.components.querytracker.QueryProgressTracker)
    * Raphtory Client extends scheduler, queries return QueryProgressTracker, not threaded worker
    * @param graphAlgorithm Graph algorithm to use for query
    * @param outputFormat Type of output format for storing results
    * @param timestamp Timestamp of query
    * @return a tracker tied to the job status
    * */
  def pointQuery(
                  graphAlgorithm: GraphAlgorithm,
                  outputFormat: OutputFormat,
                  timestamp: Long
                ): QueryProgressTracker = pointQuery(graphAlgorithm, outputFormat, timestamp,  List())

  /** Queries for a range of timestamps, returns [`QueryProgressTracker`](com.raphtory.components.querytracker.QueryProgressTracker)
    * @param graphAlgorithm  Graph algorithm to use for query
    * @param outputFormat Type of output format for storing results
    * @param start Start timestamp of duration for query
    * @param end End timestamp of duration for query
    * @param increment Step size of duration for re-running range query
    * @param windows If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`
    * @return a tracker tied to the job status
    * */
  def rangeQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobName = graphAlgorithm.getClass.getCanonicalName.split("\\.").last
    new TemporalGraph(Query(), querySender, conf)
      .range(start, end, increment)
      .window(windows, Alignment.END)
      .execute(graphAlgorithm)
      .writeTo(outputFormat, jobName)
  }

  /** Runs query for latest available timestamp, returns [`QueryProgressTracker`](com.raphtory.components.querytracker.QueryProgressTracker)
    * @param graphAlgorithm Graph algorithm to use for query
    * @param outputFormat Type of output format for storing results
    * @param increment Step size of duration for re-running range query
    * @param windows If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`
    * @return a tracker tied to the job status
    * */
  def liveQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobName = graphAlgorithm.getClass.getCanonicalName.split("\\.").last
    new TemporalGraph(Query(), querySender, conf)
      .walk(increment)
      .window(windows, Alignment.END)
      .execute(graphAlgorithm)
      .writeTo(outputFormat, jobName)
  }

  /** Fetch the raphtory config  */
  def getConfig(): Config = conf

  def getQuerySender(): QuerySender = querySender
}
