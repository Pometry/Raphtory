package com.raphtory

import cats.data.State
import cats.effect
import cats.effect._
import cats.effect.unsafe.implicits.global
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraph
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
import java.util.concurrent.atomic.AtomicReference
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

  case class Service(client: QuerySender, deploymentID: String, shutdown: IO[Unit], graphs: Set[String])

  private var localService: Option[Service] = None

  private lazy val globalConfHandler = new ConfigHandler()

  def localContext(): RaphtoryContext = new LocalRaphtoryContext()

  /** Creates a streaming version of a `DeployedTemporalGraph` object that can be used to express queries from.
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object for this stream
    */
  def streamIO[T](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map(),
      name: String = ""
  ): Resource[IO, TemporalGraph] = {
    val graphID = generateGraphID(name)
    deployStreamLocalGraph[IO](List(Source(spout, graphBuilder)), graphID, customConfig).map {
      case (client, config) =>
        new TemporalGraph(Query(graphID = graphID), client, config)
    }
  }

  /** Creates a streaming version of a `DeployedTemporalGraph` object that can be used to express queries from.
    *  this is unmanaged and returns [[DeployedTemporalGraph]] which implements [[AutoCloseable]]
    *
    * @param spout Spout to ingest objects of type `T` into the deployment
    * @param graphBuilder Graph builder to parse the input objects
    * @param customConfig Custom configuration for the deployment
    * @return The graph object for this stream
    */
  def stream[T](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map(),
      name: String = ""
  ): DeployedTemporalGraph = {
    val graphID                      = generateGraphID(name)
    val ((client, config), shutdown) =
      deployStreamLocalGraph[IO](List(Source(spout, graphBuilder)), graphID, customConfig).allocated.unsafeRunSync()
    new DeployedTemporalGraph(Query(graphID = graphID), client, config, shutdown)
  }

  def stream(sources: Source*): DeployedTemporalGraph = {
    val graphID                      = generateGraphID("")
    val ((client, config), shutdown) =
      deployStreamLocalGraph[IO](sources, graphID).allocated.unsafeRunSync()
    new DeployedTemporalGraph(Query(graphID = graphID), client, config, shutdown)
  }

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
  ): Resource[IO, TemporalGraph] =
    deployBatchLocalGraph[T, IO](spout, graphBuilder, customConfig).map {
      case (qs, config) =>
        new TemporalGraph(Query(), qs, config)
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
    val ((qs, config), shutdown) =
      deployBatchLocalGraph[T, IO](spout, graphBuilder, customConfig).allocated.unsafeRunSync()

    new DeployedTemporalGraph(Query(), qs, config, shutdown)

  }

  private def connectManaged(customConfig: Map[String, Any] = Map()): Resource[IO, (TopicRepository, Config)] = {
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

  /** Creates a `TemporalGraphConnection` object referencing an already deployed graph that
    * can be used to submit queries.
    *
    * @param customConfig Custom configuration for the deployment being referenced
    * @return A temporal graph object
    */
  def connectIO(customConfig: Map[String, Any] = Map()): Resource[IO, TemporalGraphConnection] =
    connectManaged(customConfig).map {
      case (topicRepo, config) =>
        new TemporalGraphConnection(Query(), new QuerySender(new Scheduler(), topicRepo, config), config, IO.unit)
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
      distributed: Boolean,
      useGlobal: Boolean = false
  ): Config = {
    val confHandler = if (useGlobal) globalConfHandler else new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig()
  }

  private def generateGraphID(name: String) = if (name.isEmpty) UUID.randomUUID().toString else name

  private def deployBatchLocalGraph[T, IO[_]](
      spout: Spout[T] = new IdentitySpout[T](),
      graphBuilder: GraphBuilder[T],
      customConfig: Map[String, Any] = Map()
  )(implicit IO: Async[IO]): Resource[IO, (QuerySender, Config)] = {
    val config         = confBuilder(customConfig, distributed = false)
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")
    val deploymentID   = config.getString("raphtory.deploy.id")
    val scheduler      = new Scheduler()
    for {
      _                  <- Prometheus[IO](prometheusPort) //FIXME: need some sync because this thing does not stop
      topicRepo          <- LocalTopicRepository(config)
      partitionIdManager <- makePartitionIdManager(config, localDeployment = true, deploymentID)
      _                  <- PartitionsManager.batchLoading(config, partitionIdManager, topicRepo, scheduler, spout, graphBuilder)
      _                  <- QueryManager(config, topicRepo)
      client             <- Resource.eval(IO.delay(new QuerySender(scheduler, topicRepo, config)))
    } yield (client, config)
  }

  private def deployStreamLocalGraph[IO[_]](
      sources: Seq[Source],
      graphID: String,
      customConfig: Map[String, Any] = Map()
  )(implicit
      IO: Async[IO]
  ): Resource[IO, (QuerySender, Config)] =
    Resource.make {
      IO.delay {
        localService.synchronized {
          val service = localService match {
            case Some(service) =>
              logger.debug("There is an existing service, adding graph")
              service.copy(graphs = service.graphs + graphID)
            case None          =>
              logger.debug("No local service deployed, deploying it and adding the graph")
              deployLocalService().copy(graphs = Set(graphID))
          }
          localService = Some(service)
          val config  = confBuilder(customConfig, distributed = false, useGlobal = true)
          val client  = service.client
          client.submitGraph(sources, graphID)
          (client, config)
        }
      }
    } { service =>
      IO.delay {
        localService.synchronized {
          val remainingGraphs = localService.get.graphs - graphID
          localService = localService.map(service => service.copy(graphs = remainingGraphs))
          if (remainingGraphs.isEmpty) {
            localService.get.shutdown.unsafeRunSync()
            logger.debug("Last graph in the service was removed, shutting down")
          }
        }
      }
    }

//    new DeployedTemporalGraph(Query(graphID = graphID), client, config, service.deploymentID, graphShutdown)

  private def deployLocalService(): Service = {
    val config          = confBuilder(distributed = false, useGlobal = true)
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
