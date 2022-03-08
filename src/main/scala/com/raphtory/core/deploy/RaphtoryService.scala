package com.raphtory.core.deploy

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * {s}`RaphtoryService[T: ClassTag: TypeTag]`
  *
  * : `RaphtoryService` for initialising spout, graphbuilder and schema
  *
  *
  * ## Methods
  *
  *    {s}`defineSpout(): Spout[T]`
  *      : Initialise Spout
  *
  *
  *    {s}`defineBuilder(): GraphBuilder[T]`
  *      : Initialise GraphBuilder
  *
  *
  *    {s}`defineSchema(): Schema[T]`
  *      : Initialise Schema

  *
  * Example Usage:
  *
  * ```{code-block} scala
  * import com.raphtory.core.components.graphbuilder.GraphBuilder
  * import com.raphtory.core.components.spout.Spout
  * import com.raphtory.core.components.spout.instance.FileSpout
  * import com.raphtory.core.deploy.RaphtoryService
  * import com.typesafe.config.Config
  * import org.apache.pulsar.client.api.Schema
  *
  * object LOTRDistributedTest extends RaphtoryService[String] {
  *   override def defineSpout(): Spout[String] = FileSpout()
  *   override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
  *   override def defineSchema(): Schema[String] = Schema.STRING
  * }
  * ```
  *
  */
abstract class RaphtoryService[T: ClassTag: TypeTag] {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def defineSpout(): Spout[T]
  def defineBuilder: GraphBuilder[T]
  def defineSchema(): Schema[T]

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
    Raphtory.createGraphBuilder(defineBuilder, defineSchema())

  def partitionDeploy(): Unit =
    Raphtory.createPartitionManager()

  def queryManagerDeploy(): Unit =
    Raphtory.createQueryManager()

}
