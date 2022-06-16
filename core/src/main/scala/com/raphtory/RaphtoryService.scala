package com.raphtory

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.Raphtory.makeIdManager
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.repositories.PulsarTopicRepository
import com.raphtory.internals.components.graphbuilder.BuildExecutorGroup
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.spout.SpoutExecutor
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
      case Right(_) => IO(ExitCode.Success)
      case Left(t)  => IO(logger.error(s"Failed to start $name", t)) *> IO(ExitCode.Error)
    }

  /**
    * Creates `Spout` to read or ingest data from resources or files, sending messages to builder
    * producers for each row. Supported spout types are FileSpout`, `ResourceSpout`,
    * `StaticGraphSpout`.
    */
  private def spoutDeploy(config: Config): Resource[IO, Unit] = {
    val metricsPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Prometheus[IO](metricsPort)
      topicRepo <- PulsarTopicRepository[IO](config)
      _         <- SpoutExecutor[IO, T](defineSpout(), config, topicRepo)
    } yield ()
  }

  private def builderDeploy(config: Config) = {
    val deploymentID = config.getString("raphtory.deploy.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                <- Prometheus[IO](metricsPort)
      topicRepo        <- PulsarTopicRepository[IO](config)
      builderIDManager <- makeIdManager[IO](config, localDeployment = false, s"/$deploymentID/builderCount")
      _                <- BuildExecutorGroup[IO, T](config, builderIDManager, topicRepo, defineBuilder)
    } yield ()

  }

  private def batchPartitionDeploy(config: Config) = {
    val deploymentID = config.getString("raphtory.deploy.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                  <- Prometheus[IO](metricsPort)
      topicRepo          <- PulsarTopicRepository[IO](config)
      partitionIDManager <- makeIdManager[IO](config, localDeployment = false, s"/$deploymentID/partitionCount")
      _                  <- PartitionsManager
                              .batchLoading[IO, T](config, partitionIDManager, topicRepo, new Scheduler(), defineSpout(), defineBuilder)
    } yield ()
  }

  private def streamingPartitionDeploy(config: Config) = {
    val deploymentID = config.getString("raphtory.deploy.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                  <- Prometheus[IO](metricsPort)
      topicRepo          <- PulsarTopicRepository[IO](config)
      partitionIDManager <- makeIdManager[IO](config, localDeployment = false, s"/$deploymentID/partitionCount")
      _                  <- PartitionsManager
                              .streaming[IO](config, partitionIDManager, topicRepo, new Scheduler())
    } yield ()
  }

  private def queryManagerDeploy(config: Config) = {
    val metricsPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Prometheus[IO](metricsPort)
      topicRepo <- PulsarTopicRepository[IO](config)
      _         <- QueryManager[IO](config, topicRepo)
    } yield ()
  }
}
