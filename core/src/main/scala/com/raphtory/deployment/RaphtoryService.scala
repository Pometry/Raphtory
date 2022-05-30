package com.raphtory.deployment

import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.spout.Spout
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/** `RaphtoryService` is used for distributed deployment of Raphtory as a service.
  * This is done by deploying each of it's core components - spout, graphbuilder, partitions and query manager
  *
  * Usage:
  *
  * {{{
  * import com.raphtory.components.graphbuilder.GraphBuilder
  * import com.raphtory.components.spout.Spout
  * import com.raphtory.components.spout.instance.FileSpout
  * import com.raphtory.deployment.RaphtoryService
  *
  * object LOTRDistributedTest extends RaphtoryService[String] {
  *   override def defineSpout(): Spout[String] = FileSpout()
  *   override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
  * }
  * }}}
  *
  *  @see [[com.raphtory.components.graphbuilder.GraphBuilder]] [[com.raphtory.components.spout.Spout]]
  */
abstract class RaphtoryService[T: ClassTag] {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Defines type of Spout to be created including `FileSpout`, `ResourceSpout` and
    * `StaticGraphSpout` for ingesting data
    */
  def defineSpout(): Spout[T]

  /** Initialise `GraphBuilder` for building graphs */
  def defineBuilder: GraphBuilder[T]

  def main(args: Array[String]): Unit =
    args(0) match {
      case "spout"                   => spoutDeploy()
      case "builder"                 => builderDeploy()
      case "partitionmanager"        => partitionDeploy(false)
      case "batchedPartitionmanager" => partitionDeploy(true)
      case "querymanager"            => queryManagerDeploy()
    }

  /** Deploy spouts by using `SpoutExecutor` to ingest data from files and resources,
    * sending messages per row to builder producers
    */
  def spoutDeploy(): Unit =
    Raphtory.createSpout[T](defineSpout())

  /** Deploys `GraphBuilder` to build graphs by adding vertices and edges using data
    * processed and ingested by the spout as tuples of rows
    */
  def builderDeploy(): Unit =
    Raphtory.createGraphBuilder(defineBuilder)

  /** Deploy partitions using Partition Manager for creating partitions as distributed
    * storage units with readers and writers. Uses Zookeeper to create partition IDs
    */
  def partitionDeploy(batched: Boolean): Unit =
    Raphtory.createPartitionManager[T](
            batchLoading = batched,
            spout = Some(defineSpout()),
            graphBuilder = Some(defineBuilder)
    )

  /** Deploy query manager creating `QueryManager` to spawn, handle and track queries in Raphtory */
  def queryManagerDeploy(): Unit =
    Raphtory.createQueryManager()

}
