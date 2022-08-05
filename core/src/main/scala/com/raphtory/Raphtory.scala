package com.raphtory

import cats.data.State
import cats.effect
import cats.effect._
import cats.effect.unsafe.implicits.global
import com.oblac.nomen.Nomen
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
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
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

  def localContext(): RaphtoryContext = new LocalRaphtoryContext()

  def quickGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph =
    new LocalRaphtoryContext().newGraph(graphID: String, customConfig: Map[String, Any])

  def quickIOGraph(
      graphID: String = createName,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, DeployedTemporalGraph] =
    new LocalRaphtoryContext().newIOGraph(graphID: String, customConfig: Map[String, Any])

  private def connectManaged(customConfig: Map[String, Any] = Map()): Resource[IO, (TopicRepository, Config)] = {
    val config         = confBuilder(customConfig)
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
      customConfig: Map[String, Any] = Map()
  ): Config =
    confBuilder(customConfig)

  private[raphtory] def confBuilder(
      customConfig: Map[String, Any] = Map()
  ): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig()
  }

  def makePartitionIdManager[IO[_]: Sync](
      config: Config,
      localDeployment: Boolean,
      graphID: String
  ): Resource[IO, IDManager] =
    if (localDeployment)
      Resource.eval(Sync[IO].delay(new LocalIDManager))
    else {
      val zookeeperAddress         = config.getString("raphtory.zookeeper.address")
      val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
      val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
      val totalPartitions: Int     = partitionServers * partitionsPerServer
      ZookeeperIDManager(zookeeperAddress, graphID, "partitionCount", poolSize = totalPartitions)
    }

  def makeBuilderIdManager[IO[_]: Sync](
      config: Config,
      localDeployment: Boolean,
      graphID: String
  ): Resource[IO, IDManager] =
    if (localDeployment)
      Resource.eval(Sync[IO].delay(new LocalIDManager))
    else {
      val zookeeperAddress = config.getString("raphtory.zookeeper.address")
      ZookeeperIDManager(zookeeperAddress, graphID, "builderCount")
    }

  protected def createName: String =
    Nomen.est().adjective().color().animal().get()
//    new DeployedTemporalGraph(Query(graphID = graphID), client, config, service.deploymentID, graphShutdown)

}
