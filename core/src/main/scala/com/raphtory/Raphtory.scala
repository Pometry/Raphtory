package com.raphtory

import cats.data.State
import cats.effect
import cats.effect._
import cats.effect.unsafe.implicits.global
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraphConnection
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.graphbuilder.BuildExecutorGroup
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.spout.SpoutExecutor
import com.raphtory.internals.management._
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.internals.management.id.ZookeeperIDManager
import com.raphtory.spouts.IdentitySpout
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.UUID
import scala.reflect.ClassTag

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

  protected case class Service(client: QuerySender, deploymentID: String, shutdown: IO[Unit], graphs: Set[String])

  private var localService: Option[Service] = None

  private lazy val confHandler = new ConfigHandler()

  /** Creates a streaming version of a `DeployedTemporalGraph` object that can be used to express queries from.
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object for this stream
    */
  def streamIO[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, DeployedTemporalGraph] =
    deployLocalGraph[T, IO](spout, graphBuilder, customConfig, batchLoading = false).map {
      case (qs, config, deploymentId) =>
        new DeployedTemporalGraph(Query(), qs, config, deploymentId, shutdown = IO.unit)
    }

  /** Creates a streaming version of a `DeployedTemporalGraph` object that can be used to express queries from.
    *  this is unmanaged and returns [[DeployedTemporalGraph]] which implements [[AutoCloseable]]
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object for this stream
    */
  def stream[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph =
    submitLocalGraph(List(Source(spout, graphBuilder)), customConfig = customConfig)

  def stream(sources: Source*): DeployedTemporalGraph = submitLocalGraph(sources)

  /** Creates a batch loading version of a `DeployedTemporalGraph` object that can be used to express
    * queries from.
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object created by this batch loader
    */
  def loadIO[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, DeployedTemporalGraph] =
    deployLocalGraph[T, IO](spout, graphBuilder, customConfig, batchLoading = true).map {
      case (qs, config, deploymentId) =>
        new DeployedTemporalGraph(Query(), qs, config, deploymentId, shutdown = IO.unit)
    }

  /** Creates a batch loading version of a `DeployedTemporalGraph` object that can be used to express
    * queries from.
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object created by this batch loader
    */
  def load[T: ClassTag](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph = {
    val ((qs, config, deploymentId), shutdown) =
      deployLocalGraph[T, IO](spout, graphBuilder, customConfig, batchLoading = true).allocated.unsafeRunSync()

    new DeployedTemporalGraph(Query(), qs, config, deploymentId, shutdown)

  }

  private def connectManaged(customConfig: Map[String, Any] = Map()) = {
    val config         = confBuilder(customConfig, distributed = true)
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Py4JServer.fromEntryPoint[IO](this, config)
      _         <- Prometheus[IO](prometheusPort)
      topicRepo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config)
    } yield (topicRepo, config)
  }

  /** Creates a `TemporalGraphConnection` object referencing an already deployed graph that
    * can be used to submit queries.
    *
    * @param customConfig Custom configuration for the deployment being referenced
    * @return A temporal graph object
    */
  def connect(customConfig: Map[String, Any] = Map()): TemporalGraphConnection = {
    val managed: IO[((TopicRepository, Config), IO[Unit])] = connectManaged(customConfig).allocated

    val ((topicRepo, config), shutdown) = managed.unsafeRunSync()
    new TemporalGraphConnection(Query(), new QuerySender(new Scheduler(), topicRepo, config), config, shutdown)
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
      distributed: Boolean = false,
      salt: Option[Int] = None
  ): Config =
    confBuilder(customConfig, salt, distributed)

  private[raphtory] def confBuilder(
      customConfig: Map[String, Any] = Map(),
      salt: Option[Int] = None,
      distributed: Boolean
  ): Config = {
    salt.foreach(s => confHandler.setSalt(s))
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig(distributed)
  }

  private def deployLocalGraph[T: ClassTag, IO[_]](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map(),
      batchLoading: Boolean
  )(implicit IO: Async[IO]): Resource[IO, (QuerySender, Config, String)] = {
    val config         = confBuilder(customConfig, distributed = false)
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")
    val deploymentID   = config.getString("raphtory.deploy.id")
    val scheduler      = new Scheduler()
    for {
      _                  <- Prometheus[IO](prometheusPort) //FIXME: need some sync because this thing does not stop
      topicRepo          <- LocalTopicRepository(config)
      partitionIdManager <- makePartitionIdManager(config, localDeployment = true, deploymentID)
      _                  <- {
        if (batchLoading)
          PartitionsManager.batchLoading(config, partitionIdManager, topicRepo, scheduler, spout, graphBuilder)
        else PartitionsManager.streaming(config, partitionIdManager, topicRepo, scheduler)
      }
      _                  <- {
        if (batchLoading) Resource.eval(IO.unit)
        else IngestionManager(deploymentID, config, topicRepo)
      }
      _                  <- QueryManager(config, topicRepo)
      client             <- Resource.eval(IO.delay(new QuerySender(scheduler, topicRepo, config)))
      _                  <- Resource.eval(IO.delay {
                              client.submitGraph(List(Source(spout, graphBuilder)), "")
                            })
    } yield (client, config, deploymentID)
  }

  private def submitLocalGraph(
      sources: Seq[Source],
      name: String = "",
      customConfig: Map[String, Any] = Map()
  ): DeployedTemporalGraph = {
    val graphID = if (name.isEmpty) UUID.randomUUID().toString else name
    val service = localService match {
      case Some(service) => service.copy(graphs = service.graphs + graphID)
      case None          => deployLocalService().copy(graphs = Set(graphID))
    }
    localService = Some(service)
    val config  = confBuilder(customConfig, distributed = false)
    val client  = service.client
    client.submitGraph(sources, graphID)

    val unusedService = IO {
      val remainingGraphs = service.graphs - graphID
      localService = localService.map(service => service.copy(graphs = remainingGraphs))
      remainingGraphs.isEmpty
    }
    val graphShutdown = unusedService.flatMap {
      case true  => service.shutdown
      case false => IO.unit
    }

    new DeployedTemporalGraph(Query(graphID = graphID), client, config, service.deploymentID, graphShutdown)
  }

  private def deployLocalService(): Service = {
    val config          = confBuilder(distributed = false)
    val scheduler       = new Scheduler()
    val prometheusPort  = config.getInt("raphtory.prometheus.metrics.port")
    val deploymentID    = config.getString("raphtory.deploy.id")
    val serviceResource = for {
      _                  <- Prometheus[IO](prometheusPort) //FIXME: need some sync because this thing does not stop
      topicRepo          <- LocalTopicRepository[IO](config)
      partitionIdManager <- makePartitionIdManager[IO](config, localDeployment = true, deploymentID)
      _                  <- PartitionsManager.streaming[IO](config, partitionIdManager, topicRepo, scheduler)
      _                  <- IngestionManager[IO](deploymentID, config, topicRepo)
      _                  <- QueryManager[IO](config, topicRepo)
    } yield new QuerySender(scheduler, topicRepo, config)

    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, deploymentID, shutdown, Set())
  }

  def makePartitionIdManager[IO[_]: Sync](
      config: Config,
      localDeployment: Boolean,
      deploymentId: String
  ): Resource[IO, IDManager] =
    if (localDeployment)
      Resource.eval(Sync[IO].delay(new LocalIDManager))
    else {
      val zookeeperAddress         = config.getString("raphtory.zookeeper.address")
      val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
      val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
      val totalPartitions: Int     = partitionServers * partitionsPerServer
      ZookeeperIDManager(zookeeperAddress, deploymentId, "partitionCount", poolSize = totalPartitions)
    }

  def makeBuilderIdManager[IO[_]: Sync](
      config: Config,
      localDeployment: Boolean,
      deploymentId: String
  ): Resource[IO, IDManager] =
    if (localDeployment)
      Resource.eval(Sync[IO].delay(new LocalIDManager))
    else {
      val zookeeperAddress = config.getString("raphtory.zookeeper.address")
      ZookeeperIDManager(zookeeperAddress, deploymentId, "builderCount")
    }

}
