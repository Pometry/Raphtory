package com.raphtory

import cats.effect._
import com.oblac.nomen.Nomen
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.context.LocalContext
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.context.RemoteContext
import com.raphtory.internals.context.LocalContext.createName
import com.raphtory.internals.management._
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.internals.management.id.ZooKeeperCounter
import com.raphtory.internals.management.id.ZookeeperLimitedPool
import com.typesafe.config.Config

import scala.collection.mutable.ArrayBuffer

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

  private val remoteConnections = ArrayBuffer[RemoteContext]()

  def newGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph =
    LocalContext.newGraph(graphID, customConfig)

  def newIOGraph(
      graphID: String = createName,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, DeployedTemporalGraph] = LocalContext.newIOGraph(graphID, customConfig)

  def getGraph(graphID: String): Option[DeployedTemporalGraph] = LocalContext.getGraph(graphID)

  def connect(deploymentID: String): RaphtoryContext = {
    val context = new RemoteContext(deploymentID)
    remoteConnections += context
    context
  }

  def closeGraphs(): Unit      = LocalContext.close()
  def closeConnections(): Unit = remoteConnections.foreach(_.close())

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

  private[raphtory] def makeIdManager[IO[_]: Sync](
      config: Config,
      localDeployment: Boolean,
      graphID: String,
      forPartitions: Boolean
  ): Resource[IO, IDManager] =
    if (localDeployment)
      Resource.eval(Sync[IO].delay(new LocalIDManager))
    else {
      val zookeeperAddress = config.getString("raphtory.zookeeper.address")
      if (forPartitions) {
        val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
        val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
        val totalPartitions: Int     = partitionServers * partitionsPerServer
        ZookeeperLimitedPool(zookeeperAddress, graphID, "partitionCount", poolSize = totalPartitions)
      }
      else
        ZooKeeperCounter(zookeeperAddress, graphID, "sourceCount")
    }

  private[raphtory] def createName: String =
    Nomen.est().adjective().color().animal().get()

}
