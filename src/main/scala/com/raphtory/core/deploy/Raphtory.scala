package com.raphtory.core.deploy

import com.raphtory.core.algorithm.TemporalGraph
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.config.ComponentFactory
import com.raphtory.core.config.ConfigHandler
import com.raphtory.core.config.MonixScheduler
import com.raphtory.core.config.PulsarController
import com.raphtory.core.client.GraphDeployment
import com.raphtory.core.client.QuerySender
import com.raphtory.core.client.RaphtoryClient
import com.raphtory.core.components.querymanager.Query
import com.raphtory.spouts.IdentitySpout
import com.typesafe.config.Config

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * {s}`Raphtory`
  *  : `Raphtory` object for creating Raphtory Components
  *
  * ## Methods
  *
  *   {s}`createGraph(spout: Spout[T] = new IdentitySpout[T](), graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): RaphtoryGraph[T]`
  *    : Creates Graph using spout, graph-builder and custom config. Returns {s}`RaphtoryGraph` object for the user to run queries. {s}`customConfig` is a key-value mapping of Raphtory parameters for Pulsar and components like partitions, etc. Refer to the example usage below.
  *
  *   {s}`createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map())`
  *    : Create Client to expose APIs for running point, range and live queries for graph algorithms in Raphtory.
  *      Client is for a {s}`deploymentID` and config {s}`customConfig` of parameters eg. Pulsar endpoint as illustrated in the example usage
  *
  *   {s}`createSpout(spout: Spout[T])`
  *    : Creates {s}`Spout` to read or ingest data from resources or files, sending messages to builder producers for each row. Supported spout types are {s}FileSpout`, {s}`ResourceSpout`, {s}`StaticGraphSpout`.
  *
  *   {s}`createGraphBuilder(builder: GraphBuilder[T])`
  *    : Creates {s}`GraphBuilder` for creating a Graph by adding and deleting vertices and edges. {s}`GraphBuilder` processes the data ingested by the spout as tuples of rows to build the graph
  *
  *   {s}`createPartitionManager()`
  *    : Creates {s}`PartitionManager` for creating partitions as distributed storage units with readers and writers. Uses Zookeeper to create partition IDs
  *
  *   {s}`createQueryManager()`
  *    : Creates {s}`QueryManager` for spawning, handling and tracking queries. Query types supported include {s}`PointQuery`, {s}`RangeQuery` and {s}`LiveQuery`
  *
  *   {s}`getDefaultConfig(customConfig: Map[String, Any] = Map()): Config`
  *    : Returns default config using {s}`ConfigFactory` for initialising parameters for running Raphtory components. This uses the default application parameters
  *
  *   {s}`confBuilder(customConfig: Map[String, Any] = Map()): Config`
  *    : Creates {s}`Config` by using the input map {s}`customConfig`
  *
  *   {s}`createSpoutExecutor(spout: Spout[T], conf: Config, pulsarController: PulsarController): SpoutExecutor[T]]`
  *    : Create spout executor for ingesting data from resources and files. Supported executors include `FileSpoutExecutor`, `StaticGraphSpoutExecutor`
  *
  * Example Usage:
  *
  * ```{code-block} scala
  *
  * import com.raphtory.core.deploy.Raphtory
  * import com.raphtory.lotrtest.LOTRGraphBuilder
  * import com.raphtory.core.components.spout.instance.ResourceSpout
  * import com.raphtory.GraphState
  * import com.raphtory.output.FileOutputFormat
  *
  * val customConfig = Map(("raphtory.pulsar.endpoint", "localhost:1234"))
  * Raphtory.createClient("deployment123", customConfig)
  * val graph = Raphtory.createGraph(ResourceSpout("resource"), LOTRGraphBuilder())
  * graph.rangeQuery(GraphState(),FileOutputFormat("/test_dir"),1, 32674, 10000, List(500, 1000, 10000))
  *
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.core.components.graphbuilder.GraphBuilder),
  *  [](com.raphtory.core.components.spout.Spout)
  *  ```
  */
object Raphtory {

  private val scheduler = new MonixScheduler().scheduler

  def streamGraph[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): GraphDeployment[T] = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val querySender      = new QuerySender(componentFactory, scheduler, pulsarController)
    new GraphDeployment[T](
            false,
            spout,
            graphBuilder,
            querySender,
            conf,
            componentFactory,
            scheduler
    )
  }

  def batchLoadGraph[T: ClassTag: TypeTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): GraphDeployment[T] = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val querySender      = new QuerySender(componentFactory, scheduler, pulsarController)
    new GraphDeployment[T](
            true,
            spout,
            graphBuilder,
            querySender,
            conf,
            componentFactory,
            scheduler
    )
  }

  def getGraph(customConfig: Map[String, Any] = Map()): TemporalGraph = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val querySender      = new QuerySender(componentFactory, scheduler, pulsarController)
    new TemporalGraph(Query(), querySender, conf)
  }

  def getLocalGraph[T: ClassTag: TypeTag](deployment: GraphDeployment[T]): TemporalGraph = {
    val conf        = deployment.getConfig()
    val querySender = deployment.getQuerySender()
    new TemporalGraph(Query(), querySender, conf)
  }

  def createClient(
      deploymentID: String = "",
      customConfig: Map[String, Any] = Map()
  ): RaphtoryClient = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val querySender      = new QuerySender(componentFactory, scheduler, pulsarController)
    new RaphtoryClient(querySender, conf)
  }

  def createSpout[T](spout: Spout[T]): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.spout(spout, false, scheduler)
  }

  def createGraphBuilder[T: ClassTag](
      builder: GraphBuilder[T]
  ): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.builder(builder, false, scheduler)
  }

  def createPartitionManager[T: ClassTag](
      batchLoading: Boolean = false,
      spout: Option[Spout[T]] = None,
      graphBuilder: Option[GraphBuilder[T]] = None
  ): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.partition(scheduler, batchLoading, spout, graphBuilder)
  }

  def createQueryManager(): Unit = {
    val conf             = confBuilder()
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    componentFactory.query(scheduler)
  }

  def getDefaultConfig(customConfig: Map[String, Any] = Map()): Config =
    confBuilder(customConfig)

  private def confBuilder(customConfig: Map[String, Any] = Map()): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig
  }

}
