package com.raphtory.deployment

import com.raphtory.algorithms.api.DeployedTemporalGraph
import com.raphtory.algorithms.api.TemporalGraph
import com.raphtory.algorithms.api.TemporalGraphConnection
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.raphtory.config.ComponentFactory
import com.raphtory.config.ConfigHandler
import com.raphtory.config.MonixScheduler
import com.raphtory.client.GraphDeployment
import com.raphtory.client.QuerySender
import com.raphtory.communication.topicRepositories.PulsarAkkaTopicRepository
import com.raphtory.communication.topicRepositories.PulsarTopicRepository
import com.raphtory.components.querymanager.Query
import com.raphtory.spouts.IdentitySpout
import com.typesafe.config.Config

import scala.reflect.ClassTag
import scala.reflect.classTag
import scala.reflect.runtime.universe._

/**  `Raphtory` object for creating Raphtory Components
  *
  * Usage:
  * {{{
  * import com.raphtory.deployment.Raphtory
  * import com.raphtory.components.spout.instance.ResourceSpout
  * import com.raphtory.GraphState
  * import com.raphtory.output.FileOutputFormat
  *
  * val builder = new YourGraphBuilder()
  * val customConfig = Map(("raphtory.pulsar.endpoint", "localhost:1234"))
  * val graph = Raphtory.stream(ResourceSpout("resource"), builder, customConfig)
  * graph
  *   .range(1, 32674, 10000)
  *   .windows(List(500, 1000, 10000))
  *   .execute(GraphState())
  *   .writeTo(FileOutputFormat("/test_dir"))
  *
  * graph.deployment.stop()
  * }}}
  * @see [[com.raphtory.components.graphbuilder.GraphBuilder]]
  *  [[com.raphtory.components.spout.Spout]]
  *  [[com.raphtory.algorithms.api.DeployedTemporalGraph]]
  *  [[com.raphtory.algorithms.api.TemporalGraph]]
  */
object Raphtory {
  private lazy val javaPy4jGatewayServer = new Py4JServer(this)

  /** Creates a streaming version of a `DeployedTemporalGraph` object that can be used to express queries from and to access the deployment
    * using the given `spout`, `graphBuilder` and `customConfig`.
    *
    * @param spout Spout to use to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to use to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return the graph object for this stream
    */
  def stream[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph =
    deployLocalGraph(spout, graphBuilder, customConfig, false)

  /** Creates a batch loading version of a `DeployedTemporalGraph` object that can be used to express
    * queries from and to access the deployment using the given `spout`, `graphBuilder` and `customConfig`.
    *
    * @param spout Spout to use to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to use to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return the graph object created by this batch loader
    */
  def batchLoad[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph =
    deployLocalGraph(spout, graphBuilder, customConfig, true)

  /** Creates `TemporalGraph` object referencing an already deployed graph that
    * can be used to express queries from using the given `customConfig`.
    *
    * @param customConfig Custom configuration for the deployment being referenced
    * @return a temporal graph object
    */
  def deployedGraph(customConfig: Map[String, Any] = Map()): TemporalGraphConnection = {
    val scheduler        = new MonixScheduler()
    val conf             = confBuilder(customConfig)
    javaPy4jGatewayServer.start(conf)
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    val querySender      = new QuerySender(componentFactory, scheduler, topics)
    new TemporalGraphConnection(Query(), querySender, conf, scheduler, topics)
  }

  /** Creates `Spout` to read or ingest data from resources or files, sending messages to builder
    * producers for each row. Supported spout types are FileSpout`, `ResourceSpout`,
    * `StaticGraphSpout`.
    */
  def createSpout[T](spout: Spout[T]): Unit = {
    val scheduler        = new MonixScheduler()
    val conf             = confBuilder()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.spout(spout, false, scheduler)
  }

  /** Creates `GraphBuilder` for creating a Graph by adding and deleting vertices and edges.
    * `GraphBuilder` processes the data ingested by the spout as tuples of rows to build the graph
    */
  def createGraphBuilder[T: ClassTag](
      builder: GraphBuilder[T]
  ): Unit = {
    val scheduler        = new MonixScheduler()
    val conf             = confBuilder()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.builder(builder, false, scheduler)
  }

  /** Creates `PartitionManager` for creating partitions as distributed storage units with readers and
    * writers. Uses Zookeeper to create partition IDs
    */
  def createPartitionManager[T: ClassTag](
      batchLoading: Boolean = false,
      spout: Option[Spout[T]] = None,
      graphBuilder: Option[GraphBuilder[T]] = None
  ): Unit = {
    val scheduler        = new MonixScheduler()
    val conf             = confBuilder()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.partition(scheduler, batchLoading, spout, graphBuilder)
  }

  /** Creates `QueryManager` for spawning, handling and tracking queries. Query types
    * supported include `PointQuery`, `RangeQuery` and `LiveQuery`
    */
  def createQueryManager(): Unit = {
    val scheduler        = new MonixScheduler()
    val conf             = confBuilder()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.query(scheduler)
  }

  /** Returns default config using `ConfigFactory` for initialising parameters for
    * running Raphtory components. This uses the default application parameters
    */
  def getDefaultConfig(customConfig: Map[String, Any] = Map()): Config =
    confBuilder(customConfig)

  private def confBuilder(customConfig: Map[String, Any] = Map()): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig
  }

  private def deployLocalGraph[T: ClassTag: TypeTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map(),
      batchLoading: Boolean
  ) = {
    val scheduler        = new MonixScheduler()
    val conf             = confBuilder(customConfig)
    javaPy4jGatewayServer.start(conf)
    val topics           = PulsarAkkaTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics, true)
    val querySender      = new QuerySender(componentFactory, scheduler, topics)
    val deployment       = new GraphDeployment[T](
            batchLoading,
            spout,
            graphBuilder,
            conf,
            componentFactory,
            scheduler
    )
    new DeployedTemporalGraph(Query(), querySender, deployment.stop, conf)
  }
}
