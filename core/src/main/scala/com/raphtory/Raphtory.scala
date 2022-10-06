package com.raphtory

import cats.effect._
import com.oblac.nomen.Nomen
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.context.LocalContext
import com.raphtory.internals.context.RaphtoryContext
import com.raphtory.internals.context.RemoteContext
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
  * @see [[GraphBuilder GraphBuilder]]
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

  def connect(address: String = "", port: Int = 0): RaphtoryContext = {
    val context = new RemoteContext(address, port)
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

  private[raphtory] def makeLocalIdManager[IO[_]: Sync] =
    Resource.eval(Sync[IO].delay(new LocalIDManager))

  private[raphtory] def makePartitionIDManager[IO[_]: Sync](config: Config) = {
    val zookeeperAddress         = config.getString("raphtory.zookeeper.address")
    val partitionServers: Int    = config.getInt("raphtory.partitions.serverCount")
    val partitionsPerServer: Int = config.getInt("raphtory.partitions.countPerServer")
    val totalPartitions: Int     = partitionServers * partitionsPerServer
    ZookeeperLimitedPool(zookeeperAddress, "partitionCount", poolSize = totalPartitions)
  }

  private[raphtory] def makeSourceIDManager[IO[_]: Sync](config: Config) = { //Currently no reason to use as the head node is the authority
    val zookeeperAddress = config.getString("raphtory.zookeeper.address")
    ZooKeeperCounter(zookeeperAddress, "sourceCount")
  }

  private[raphtory] def createName: String =
    Nomen.est().adjective().color().animal().get()

}
