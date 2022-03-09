package com.raphtory.core.deploy

import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.spout.Spout
import com.typesafe.scalalogging.Logger
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

abstract class RaphtoryService[T: ClassTag] {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def defineSpout(): Spout[T]
  def defineBuilder: GraphBuilder[T]

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
    Raphtory.createPartitionManager()

  def queryManagerDeploy(): Unit =
    Raphtory.createQueryManager()

}
