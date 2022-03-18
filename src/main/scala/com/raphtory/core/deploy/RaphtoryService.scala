package com.raphtory.core.deploy

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * {s}`RaphtoryService[T]`
  *
  *  : `RaphtoryService` is used for distributed deployment of Raphtory as a service. This is done by deploying each of it's core components - spout, graphbuilder, partitions and query manager
  *
  * ## Methods
  *
  *    {s}`defineSpout(): Spout[T]`
  *      : Defines type of Spout to be created including {s}`FileSpout`, {s}`ResourceSpout` and {s}`StaticGraphSpout` for ingesting data
  *
  *    {s}`defineBuilder(): GraphBuilder[T]`
  *      : Initialise {s}`GraphBuilder` for building graphs
  *
  *    {s}`spoutDeploy(): Unit`
  *      : Deploy spouts by using {s}`SpoutExecutor` to ingest data from files and resources, sending messages per row to builder producers
  *
  *    {s}`builderDeploy(): Unit`
  *      : Deploys {s}`GraphBuilder` to build graphs by adding vertices and edges using data processed and ingested by the spout as tuples of rows
  *
  *   {s}`partitionDeploy(): Unit`
  *      : Deploy partitions using Partition Manager for creating partitions as distributed storage units with readers and writers. Uses Zookeeper to create partition IDs
  *
  *   {s}`queryManagerDeploy(): Unit`
  *      : Deploy query manager creating {s}`QueryManager` to spawn, handle and track queries in Raphtory
  * Example Usage:
  *
  * ```{code-block} scala
  * import com.raphtory.core.components.graphbuilder.GraphBuilder
  * import com.raphtory.core.components.spout.Spout
  * import com.raphtory.core.components.spout.instance.FileSpout
  * import com.raphtory.core.deploy.RaphtoryService
  *
  * object LOTRDistributedTest extends RaphtoryService[String] {
  *   override def defineSpout(): Spout[String] = FileSpout()
  *   override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
  * }
  * ```
  *
  *  ```{seealso}
  *  [](com.raphtory.core.components.graphbuilder.GraphBuilder),
  *  [](com.raphtory.core.components.spout.Spout)
  *  ```
  */
abstract class RaphtoryService[T: ClassTag] {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def defineSpout(): Spout[T]
  def defineBuilder: GraphBuilder[T]
  def batchIngestion(): Boolean

  def main(args: Array[String]): Unit =
    args(0) match {
      case "spout"            => spoutDeploy()
      case "builder"          => builderDeploy()
      case "partitionmanager" => partitionDeploy()
      case "querymanager"     => queryManagerDeploy()
    }

  def spoutDeploy(): Unit =
    Raphtory.createSpout[T](defineSpout())

  def builderDeploy(): Unit =
    Raphtory.createGraphBuilder(defineBuilder)

  def partitionDeploy(): Unit =
    Raphtory.createPartitionManager[T](
            batchLoading = batchIngestion(),
            spout = Some(defineSpout()),
            graphBuilder = Some(defineBuilder)
    )

  def queryManagerDeploy(): Unit =
    Raphtory.createQueryManager()

}
