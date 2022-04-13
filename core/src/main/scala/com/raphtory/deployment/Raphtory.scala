package com.raphtory.deployment

import com.raphtory.algorithms.api.DeployedTemporalGraph
import com.raphtory.algorithms.api.TemporalGraph
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.config.ComponentFactory
import com.raphtory.config.ConfigHandler
import com.raphtory.config.MonixScheduler
import com.raphtory.config.PulsarController
import com.raphtory.client.GraphDeployment
import com.raphtory.client.QuerySender
import com.raphtory.client.RaphtoryClient
import com.raphtory.components.querymanager.Query
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
  *   {s}`stream[T: TypeTag: ClassTag](spout: Spout[T] = new IdentitySpout[T](),graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): DeployedTemporalGraph`
  *    : Creates a streaming version of a {s}`DeployedTemporalGraph` object that can be used to express queries from and to access the deployment
  *    using the given {s}`spout`, {s}`graphBuilder` and {s}`customConfig`.
  *
  *      {s}`spout: Spout[T]`
  *      : Spout to use to ingest objects of type {s}`T` into the deployment
  *
  *      {s}`graphBuilder: GraphBuilder[T]`
  *      : Graph builder to use to parse the input objects
  *
  *      {s}`customConfig: Map[String, Any]`
  *      : Custom configuration for the deployment
  *
  *   {s}`batchLoad[T: TypeTag: ClassTag](spout: Spout[T] = new IdentitySpout[T](),graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): DeployedTemporalGraph`
  *    : Creates a batch loading version of a {s}`DeployedTemporalGraph` object that can be used to express queries from and to access the deployment
  *    using the given {s}`spout`, {s}`graphBuilder` and {s}`customConfig`.
  *
  *      {s}`spout: Spout[T]`
  *      : Spout to use to ingest objects of type {s}`T` into the deployment
  *
  *      {s}`graphBuilder: GraphBuilder[T]`
  *      : Graph builder to use to parse the input objects
  *
  *      {s}`customConfig: Map[String, Any]`
  *      : Custom configuration for the deployment
  *
  *   {s}`deployedGraph(customConfig: Map[String, Any] = Map()): TemporalGraph`
  *    : Creates {s}`TemporalGraph` object referencing an already deployed graph that can be used to express queries from
  *    using the given {s}`customConfig`.
  *
  *      {s}`customConfig: Map[String, Any]`
  *      : Custom configuration for the deployment being referenced
  *
  *   {s}`streamGraph[T: TypeTag: ClassTag](spout: Spout[T] = new IdentitySpout[T](),graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): DeployedTemporalGraph`
  *    : Creates a streaming version of a {s}`graphDeployment` object
  *    using the given {s}`spout`, {s}`graphBuilder` and {s}`customConfig`.
  *
  *      {s}`spout: Spout[T]`
  *      : Spout to use to ingest objects of type {s}`T` into the deployment
  *
  *      {s}`graphBuilder: GraphBuilder[T]`
  *      : Graph builder to use to parse the input objects
  *
  *      {s}`customConfig: Map[String, Any]`
  *      : Custom configuration for the deployment
  *
  *   {s}`batchLoadGraph[T: TypeTag: ClassTag](spout: Spout[T] = new IdentitySpout[T](),graphBuilder: GraphBuilder[T], customConfig: Map[String, Any] = Map()): DeployedTemporalGraph`
  *    : Creates a batch loading version of a {s}`graphDeployment`
  *    using the given {s}`spout`, {s}`graphBuilder` and {s}`customConfig`.
  *
  *      {s}`spout: Spout[T]`
  *      : Spout to use to ingest objects of type {s}`T` into the deployment
  *
  *      {s}`graphBuilder: GraphBuilder[T]`
  *      : Graph builder to use to parse the input objects
  *
  *      {s}`customConfig: Map[String, Any]`
  *      : Custom configuration for the deployment
  *
  *   {s}`createClient(deploymentID: String = "", customConfig: Map[String, Any] = Map()): RaphtoryClient`
  *    : Creates a {s}`RaphtoryClient` object referencing an already deployed graph
  *    that can be used to express point range and live queries
  *    using the given {s}`customConfig`.
  *
  *      {s}`customConfig: Map[String, Any]`
  *      : Custom configuration for the deployment being referenced
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
  * Example Usage:
  *
  * ```{code-block} scala
  * import com.raphtory.deployment.Raphtory
  * import com.raphtory.components.spout.instance.ResourceSpout
  * import com.raphtory.GraphState
  * import com.raphtory.output.FileOutputFormat
  *
  * val builder = ???
  * val customConfig = Map(("raphtory.pulsar.endpoint", "localhost:1234"))
  * val graph = Raphtory.stream(ResourceSpout("resource"), builder, customConfig)
  * graph
  *   .range(1, 32674, 10000)
  *   .windows(List(500, 1000, 10000))
  *   .execute(GraphState())
  *   .writeTo(FileOutputFormat("/test_dir"))
  *
  * graph.deployment.stop()
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.components.graphbuilder.GraphBuilder),
  *  [](com.raphtory.components.spout.Spout),
  *  [](com.raphtory.algorithms.api.DeployedTemporalGraph),
  *  [](com.raphtory.algorithms.api.TemporalGraph)
  *  ```
  */
object Raphtory {

  private val scheduler = new MonixScheduler().scheduler

  def stream[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph = {
    val deployment = Raphtory.streamGraph(spout, graphBuilder, customConfig)
    getLocalGraph(deployment)
  }

  def batchLoad[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph = {
    val deployment = Raphtory.batchLoadGraph(spout, graphBuilder, customConfig)
    getLocalGraph(deployment)
  }

  def deployedGraph(customConfig: Map[String, Any] = Map()): TemporalGraph = {
    val conf             = confBuilder(customConfig)
    val pulsarController = new PulsarController(conf)
    val componentFactory = new ComponentFactory(conf, pulsarController)
    val querySender      = new QuerySender(componentFactory, scheduler, pulsarController)
    new TemporalGraph(Query(), querySender, conf)
  }

  @deprecated("Use Raphtory.stream(...) instead", "Raphtory 0.5.0")
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

  @deprecated("Use Raphtory.batchLoad(...) instead", "Raphtory 0.5.0")
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

  @deprecated("Use Raphtory.deployedGraph(...) instead", "Raphtory 0.5.0")
  def createClient(customConfig: Map[String, Any] = Map()): RaphtoryClient = {
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

  private def getLocalGraph[T: ClassTag: TypeTag](deployment: GraphDeployment[T]) = {
    val conf        = deployment.getConfig()
    val querySender = deployment.getQuerySender()
    new DeployedTemporalGraph(Query(), querySender, deployment.stop, conf)
  }
}
