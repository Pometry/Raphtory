package com.raphtory

import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraphConnection
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.repositories.PulsarAkkaTopicRepository
import com.raphtory.internals.communication.repositories.PulsarTopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.ComponentFactory
import com.raphtory.internals.management.ConfigHandler
import com.raphtory.internals.management.GraphDeployment
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.Py4JServer
import com.raphtory.internals.management.QuerySender
import com.raphtory.spouts.IdentitySpout
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import io.prometheus.client.exporter.HTTPServer
import org.slf4j.LoggerFactory

import java.io.IOException
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**  `Raphtory` object for creating Raphtory Components
  *
  * @example
  * {{{
  * import com.raphtory.Raphtory
  * import com.raphtory.spouts.FileSpout
  * import com.raphtory.api.analysis.graphstate.GraphState
  * import com.raphtory.sinks.FileSink
  *
  * val builder = new YourGraphBuilder()
  * val graph = Raphtory.stream(FileSpout("/path/to/your/file"), builder)
  * graph
  *   .range(1, 32674, 10000)
  *   .windows(List(500, 1000, 10000))
  *   .execute(GraphState())
  *   .writeTo(FileSink("/test_dir"))
  *
  * graph.deployment.stop()
  * }}}
  *
  * @see [[api.input.GraphBuilder GraphBuilder]]
  *      [[api.input.Spout Spout]]
  *      [[api.analysis.graphview.DeployedTemporalGraph DeployedTemporalGraph]]
  *      [[api.analysis.graphview.TemporalGraph TemporalGraph]]
  */
object Raphtory {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private lazy val javaPy4jGatewayServer           = new Py4JServer(this)
  private var prometheusServer: Option[HTTPServer] = None

  /** Creates a streaming version of a `DeployedTemporalGraph` object that can be used to express queries from.
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object for this stream
    */
  def stream[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph =
    deployLocalGraph(spout, graphBuilder, customConfig, false)

  /** Creates a batch loading version of a `DeployedTemporalGraph` object that can be used to express
    * queries from.
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object created by this batch loader
    */
  def load[T: TypeTag: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph =
    deployLocalGraph(spout, graphBuilder, customConfig, true)

  /** Creates a `TemporalGraphConnection` object referencing an already deployed graph that
    * can be used to submit queries.
    *
    * @param customConfig Custom configuration for the deployment being referenced
    * @return A temporal graph object
    */
  def connect(customConfig: Map[String, Any] = Map()): TemporalGraphConnection = {
    val scheduler          = new Scheduler()
    val conf               = confBuilder(customConfig, true)
    val activePythonServer = conf.getBoolean("raphtory.python.active")
    if (activePythonServer)
      javaPy4jGatewayServer.start(conf)
    startPrometheus(conf.getInt("raphtory.prometheus.metrics.port"))
    allowIllegalReflection()
    val topics             = PulsarTopicRepository(conf)
    val componentFactory   = new ComponentFactory(conf, topics)
    val querySender        = new QuerySender(componentFactory, scheduler, topics)
    new TemporalGraphConnection(Query(), querySender, conf, scheduler, topics)
  }

  /** Returns a default config using `ConfigFactory` for initialising parameters for
    * running Raphtory components. This uses the default application parameters
    *
    * @param customConfig Custom configuration for the deployment
    * @param distributed Whether the deployment is distributed or not
    * @return An immutable config object
    */
  def getDefaultConfig(
      customConfig: Map[String, Any] = Map(),
      distributed: Boolean = false
  ): Config =
    confBuilder(customConfig, distributed)

  private def confBuilder(customConfig: Map[String, Any] = Map(), distributed: Boolean): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig(distributed)
  }

  private[raphtory] def createSpout[T](spout: Spout[T]): Unit = {
    val scheduler        = new Scheduler()
    val conf             = confBuilder(distributed = true)
    startPrometheus(conf.getInt("raphtory.prometheus.metrics.port"))
    allowIllegalReflection()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.spout(spout, false, scheduler)
  }

  private[raphtory] def createGraphBuilder[T: ClassTag](
      builder: GraphBuilder[T]
  ): Unit = {
    val scheduler        = new Scheduler()
    val conf             = confBuilder(distributed = true)
    startPrometheus(conf.getInt("raphtory.prometheus.metrics.port"))
    allowIllegalReflection()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.builder(builder, false, scheduler)
  }

  private[raphtory] def createPartitionManager[T: ClassTag](
      batchLoading: Boolean = false,
      spout: Option[Spout[T]] = None,
      graphBuilder: Option[GraphBuilder[T]] = None
  ): Unit = {
    val scheduler        = new Scheduler()
    val conf             = confBuilder(distributed = true)
    startPrometheus(conf.getInt("raphtory.prometheus.metrics.port"))
    allowIllegalReflection()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.partition(scheduler, batchLoading, spout, graphBuilder)
  }

  private[raphtory] def createQueryManager(): Unit = {
    val scheduler        = new Scheduler()
    val conf             = confBuilder(distributed = true)
    startPrometheus(conf.getInt("raphtory.prometheus.metrics.port"))
    allowIllegalReflection()
    val topics           = PulsarTopicRepository(conf)
    val componentFactory = new ComponentFactory(conf, topics)
    componentFactory.query(scheduler)
  }

  private def newPrometheusServer(prometheusPort: Int): Unit =
    try prometheusServer = Some(new HTTPServer(prometheusPort))
    catch {
      case e: IOException =>
        logger.error(
                s"Cannot create prometheus server as port $prometheusPort is already bound, " +
                  s"this could be you have multiple raphtory instances running on the same machine. "
        )
    }

  private[raphtory] def startPrometheus(prometheusPort: Int): Unit =
    synchronized {
      prometheusServer match {
        case Some(server) =>
          if (server.getPort != prometheusPort)
            logger.warn(
                    s"This Raphtory Instance is already running a Prometheus Server on port ${server.getPort}."
            )
        case None         => newPrometheusServer(prometheusPort)
      }
    }

  private def deployLocalGraph[T: ClassTag: TypeTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map(),
      batchLoading: Boolean
  ) = {
    val scheduler        = new Scheduler()
    val conf               = confBuilder(customConfig, distributed = false)
    val activePythonServer = conf.getBoolean("raphtory.python.active")
    if (activePythonServer)
      javaPy4jGatewayServer.start(conf)
    startPrometheus(conf.getInt("raphtory.prometheus.metrics.port"))
    allowIllegalReflection()
    val queriesEnabled     = conf.getBoolean("raphtory.query.localEnabled")
    val topics             =
      if (queriesEnabled)
        PulsarTopicRepository(conf)
      else PulsarAkkaTopicRepository(conf)
    val componentFactory   = new ComponentFactory(conf, topics, true)
    val querySender        = new QuerySender(componentFactory, scheduler, topics)
    val deployment         = new GraphDeployment[T](
            batchLoading,
            spout,
            graphBuilder,
            conf,
            componentFactory,
            scheduler
    )
    new DeployedTemporalGraph(Query(), querySender, deployment, conf)
  }

  def shutdown(): Unit = {
    prometheusServer.foreach(_.close())
    javaPy4jGatewayServer.shutdown()
  }

  private[raphtory] def allowIllegalReflection() = {
    import java.lang.reflect.Field

    try { // Turn off illegal access log messages.
      val loggerClass = Class.forName("jdk.internal.module.IllegalAccessLogger")
      val loggerField = loggerClass.getDeclaredField("logger")
      val unsafeClass = Class.forName("sun.misc.Unsafe")
      val unsafeField = unsafeClass.getDeclaredField("theUnsafe")
      unsafeField.setAccessible(true)
      val unsafe      = unsafeField.get(null)
      val offset      =
        unsafeClass
          .getMethod("staticFieldOffset", classOf[Field])
          .invoke(unsafe, loggerField)
          .asInstanceOf[Long]
      unsafeClass
        .getMethod("putObjectVolatile", classOf[Object], classOf[Long], classOf[Object])
        .invoke(unsafe, loggerClass, offset, null)
    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
  }
}
