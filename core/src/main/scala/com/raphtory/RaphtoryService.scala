package com.raphtory

import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/** `RaphtoryService` is used for distributed deployment of Raphtory as a service.
  * This is done by deploying each of it's core components - spout, graphbuilder, partitions and query manager across a cluster
  *
  * @example
  * {{{
  * import com.raphtory.components.graphbuilder.GraphBuilder
  * import com.raphtory.components.spout.Spout
  * import com.raphtory.components.spout.instance.FileSpout
  * import com.raphtory.RaphtoryService
  *
  * object LOTRDistributedTest extends RaphtoryService[String] {
  *   override def defineSpout(): Spout[String] = FileSpout()
  *   override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
  * }}}
  *
  *  @see [[api.input.GraphBuilder]] [[api.input.Spout]]
  */
abstract class RaphtoryService[T: ClassTag] {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Defines type of Spout to be created for ingesting data
    */
  def defineSpout(): Spout[T]

  /** Initialise `GraphBuilder` for building graphs */
  def defineBuilder: GraphBuilder[T]

  def main(args: Array[String]): Unit =
    args(0) match {
      case "spout"                 => spoutDeploy()
      case "builder"               => builderDeploy()
      case "partitionmanager"      => partitionDeploy(false)
      case "batchpartitionmanager" => partitionDeploy(true)
      case "querymanager"          => queryManagerDeploy()
    }

  private def spoutDeploy(): Unit =
    Raphtory.createSpout[T](defineSpout())

  private def builderDeploy(): Unit =
    Raphtory.createGraphBuilder(defineBuilder)

  private def partitionDeploy(batched: Boolean): Unit =
    Raphtory.createPartitionManager[T](
            batchLoading = batched,
            spout = Some(defineSpout()),
            graphBuilder = Some(defineBuilder)
    )

  private def queryManagerDeploy(): Unit =
    Raphtory.createQueryManager()

}
