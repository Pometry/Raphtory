package com.raphtory.core.client

import com.raphtory.core.algorithm.GraphAlgorithm
import com.raphtory.core.algorithm.OutputFormat
import com.raphtory.core.components.Component
import com.raphtory.core.components.querymanager.LiveQuery
import com.raphtory.core.components.querymanager.PointQuery
import com.raphtory.core.components.querymanager.RangeQuery
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.PulsarController
import com.raphtory.serialisers.PulsarKryoSerialiser
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Producer
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.slf4j.LoggerFactory

/**
  * {s}`RaphtoryClient`
  *    : Raphtory Client exposes query APIs for point, range and live queries
  *
  *  {s}`RaphtoryClient` should not be created directly. To create a {s}`RaphtoryClient` use
  *  {s}`Raphtory.createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`.
  *
  *  ## Methods
  *
  *    {s}`pointQuery(graphAlgorithm: GraphAlgorithm, outputFormat: OutputFormat, timestamp: Long, windows: List[Long] = List()): QueryProgressTracker`
  *      : Point query for querying at a specific timestamp, returns [{s}`QueryProgressTracker`](com.raphtory.core.components.querytracker.QueryProgressTracker)
  *
  *        {s}`graphAlgorithm: GraphAlgorithm`
  *           : Graph algorithm to use for query
  *
  *        {s}`outputFormat: OutputFormat`
  *           : Type of output format for storing results
  *
  *        {s}`timestamp: Long`
  *           : Timestamp of query
  *
  *        {s}`windows: List[Long] = List()`
  *           : If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`
  *
  *    {s}`rangeQuery(graphAlgorithm: GraphAlgorithm, outputFormat: OutputFormat, start: Long, end: Long, increment: Long, windows: List[Long] = List()): QueryProgressTracker`
  *      : Queries for a range of timestamps, returns [{s}`QueryProgressTracker`](com.raphtory.core.components.querytracker.QueryProgressTracker)
  *
  *        {s}`graphAlgorithm: GraphAlgorithm`
  *           : Graph algorithm to use for query
  *
  *        {s}`outputFormat: OutputFormat`
  *           : Type of output format for storing results
  *
  *        {s}`start: Long`
  *           : Start timestamp of duration for query
  *
  *        {s}`end: Long`
  *           : End timestamp of duration for query
  *
  *        {s}`increment: Long`
  *           : Step size of duration for re-running range query
  *
  *        {s}`windows: List[Long] = List()`
  *           : If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`
  *
  *    {s}`liveQuery(graphAlgorithm: GraphAlgorithm, outputFormat: OutputFormat, increment: Long, windows: List[Long] = List()): QueryProgressTracker`
  *      : Runs query for latest available timestamp, returns [{s}`QueryProgressTracker`](com.raphtory.core.components.querytracker.QueryProgressTracker)
  *
  *        {s}`graphAlgorithm: GraphAlgorithm`
  *           : Graph algorithm to use for query
  *
  *        {s}`outputFormat: OutputFormat`
  *           : Type of output format for storing results
  *
  *        {s}`increment: Long`
  *           : Step size of duration for re-running range query
  *
  *        {s}`windows: List[Long] = List()`
  *           : If specified, run query for each `window` in `windows`, restricted to data between `timestamp - window` and `timestamp`
  *
  *    {s}`getConfig(): Config`
  *      : Fetch raphtory config
  *
  *    {s}`getID(algorithm: GraphAlgorithm): String`
  *     : Fetch ID for graph algorithm
  *
  *        {s}`graphAlgorithm: GraphAlgorithm`
  *           : Graph algorithm to use for query
  *
  * Usage while querying:
  *
  * ```{code-block} scala
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.core.algorithm.OutputFormat
  * import com.raphtory.core.components.graphbuilder.GraphBuilder
  * import com.raphtory.core.components.spout.Spout
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
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.core.algorithm.GraphAlgorithm), [](com.raphtory.core.components.querytracker.QueryProgressTracker),
  *  [](com.raphtory.output.FileOutputFormat), [](com.raphtory.output.PulsarOutputFormat),
  *  [](com.raphtory.core.deploy.Raphtory)
  *  ```
  */
private[core] class RaphtoryClient(
    private val deploymentID: String,
    private val conf: Config,
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val pulsarController: PulsarController
) {

  private var internalID = deploymentID

  private val kryo                                 = PulsarKryoSerialiser()
  implicit private val schema: Schema[Array[Byte]] = Schema.BYTES
  val logger: Logger                               = Logger(LoggerFactory.getLogger(this.getClass))

  // Raphtory Client extends scheduler, queries return QueryProgressTracker, not threaded worker
  def pointQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      timestamp: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobID = getID(graphAlgorithm)
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(
            PointQuery(jobID, graphAlgorithm, timestamp, windows, outputFormat)
    )
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  def rangeQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      start: Long,
      end: Long,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobID = getID(graphAlgorithm)
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(
            RangeQuery(jobID, graphAlgorithm, start, end, increment, windows, outputFormat)
    )
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  def liveQuery(
      graphAlgorithm: GraphAlgorithm,
      outputFormat: OutputFormat,
      increment: Long,
      windows: List[Long] = List()
  ): QueryProgressTracker = {
    val jobID = getID(graphAlgorithm)
    pulsarController.toQueryManagerProducer sendAsync kryo.serialise(
            LiveQuery(jobID, graphAlgorithm, increment, windows, outputFormat)
    )
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  def getConfig(): Config = conf

  private def getID(algorithm: GraphAlgorithm): String =
    try {
      val path = algorithm.getClass.getCanonicalName.split("\\.")
      path(path.size - 1) + "_" + System.currentTimeMillis()
    }
    catch {
      case e: NullPointerException => "Anon_Func_" + System.currentTimeMillis()
    }

}
