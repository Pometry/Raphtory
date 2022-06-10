package com.raphtory

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.repositories.PulsarAkkaClusterTopicRepository
import com.raphtory.internals.components.graphbuilder.BuildExecutorGroup
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.spout.SpoutExecutor
import com.raphtory.internals.management.BatchPartitionManager
import com.raphtory.internals.management.PartitionsManager
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/** `RaphtoryService` is used for distributed deployment of Raphtory as a service.
  * This is done by deploying each of it's core components - spout, graphbuilder, partitions and query manager across a cluster
  *
  * @example
  * {{{
  * import com.raphtory.RaphtoryService
  * import com.raphtory.api.input.GraphBuilder
  * import com.raphtory.api.input.Spout
  * import com.raphtory.spouts.FileSpout
  *
  * object LOTRDistributedTest extends RaphtoryService[String] {
  *  override def defineSpout(): Spout[String]        = FileSpout()
  *  override def defineBuilder: GraphBuilder[String] = new LOTRGraphBuilder()
  * }
  * }}}
  *
  *  @see [[api.input.GraphBuilder GraphBuilder]]
  *        [[api.input.Spout Spout]]
  */
abstract class RaphtoryService[T: ClassTag] extends IOApp {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Defines type of Spout to be created for ingesting data
    */
  def defineSpout(): Spout[T]

  /** Initialise `GraphBuilder` for building graphs */
  def defineBuilder: GraphBuilder[T]

  override def run(args: List[String]): IO[ExitCode] = {

    val config = Raphtory.confBuilder(distributed = true)
    args.head match {
      case name @ "spout"                 => useToExitCode(name, spoutDeploy(config))
      case name @ "builder"               => useToExitCode(name, builderDeploy(config))
      case name @ "partitionmanager"      => useToExitCode(name, streamingPartitionDeploy(config))
      case name @ "batchpartitionmanager" => useToExitCode(name, batchPartitionDeploy(config))
      case name @ "querymanager"          => useToExitCode(name, queryManagerDeploy(config))
    }
  }

  def useToExitCode[R](name: String, r: Resource[IO, R]): IO[ExitCode] =
    r.attempt.use {
      case Right(_) => IO.never *> IO(ExitCode.Success)
      case Left(t)  => IO(logger.error(s"Failed to start $name", t)) *> IO(ExitCode.Error)
    }

  /**
    * Creates `Spout` to read or ingest data from resources or files, sending messages to builder
    * producers for each row.
    */
  def spoutDeploy(config: Config): Resource[IO, SpoutExecutor[T]] = {
    val metricsPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Prometheus[IO](metricsPort)
      topicRepo <- PulsarAkkaClusterTopicRepository[IO](config)
      s         <- SpoutExecutor[IO, T](defineSpout(), config, topicRepo)
    } yield s
  }

  def builderDeploy(config: Config, localDeployment: Boolean = false): Resource[IO, BuildExecutorGroup] = {
    val deploymentID = config.getString("raphtory.deploy.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                <- Prometheus[IO](metricsPort)
      topicRepo        <- PulsarAkkaClusterTopicRepository[IO](config)
      builderIDManager <- Raphtory.makeBuilderIdManager[IO](config, localDeployment, deploymentID)
      beg              <- BuildExecutorGroup[IO, T](config, builderIDManager, topicRepo, defineBuilder)
    } yield beg

  }

  def batchPartitionDeploy(config: Config): Resource[IO, BatchPartitionManager[T]] = {
    val deploymentID = config.getString("raphtory.deploy.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                  <- Prometheus[IO](metricsPort)
      topicRepo          <- PulsarAkkaClusterTopicRepository[IO](config)
      partitionIDManager <- Raphtory.makePartitionIdManager[IO](config, localDeployment = false, deploymentID)
      pm                 <- PartitionsManager
                              .batchLoading[IO, T](config, partitionIDManager, topicRepo, new Scheduler(), defineSpout(), defineBuilder)
    } yield pm
  }

  def streamingPartitionDeploy(config: Config): Resource[IO, PartitionsManager] = {
    val deploymentID = config.getString("raphtory.deploy.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                  <- Prometheus[IO](metricsPort)
      topicRepo          <- PulsarAkkaClusterTopicRepository[IO](config)
      partitionIDManager <- Raphtory.makePartitionIdManager[IO](config, localDeployment = false, deploymentID)
      pm                 <- PartitionsManager
                              .streaming[IO](config, partitionIDManager, topicRepo, new Scheduler())
    } yield pm
  }

  def queryManagerDeploy(config: Config): Resource[IO, QueryManager] = {
    val metricsPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Prometheus[IO](metricsPort)
      topicRepo <- PulsarAkkaClusterTopicRepository[IO](config, seed = true)
      qm        <- QueryManager[IO](config, topicRepo)
    } yield qm
  }
}

object RaphtoryService {

  def default[T: ClassTag](spout: Spout[T], graphBuilder: GraphBuilder[T]): RaphtoryService[T] =
    new RaphtoryService[T] {

      /** Defines type of Spout to be created for ingesting data
        */
      override def defineSpout(): Spout[T] = spout

      /** Initialise `GraphBuilder` for building graphs */
      override def defineBuilder: GraphBuilder[T] = graphBuilder
    }
}
