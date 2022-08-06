package com.raphtory

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryManager
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

    val config = Raphtory.confBuilder()
    args.head match {
      case name @ "partitionmanager" => useToExitCode(name, streamingPartitionDeploy(config))
      case name @ "querymanager"     => useToExitCode(name, queryManagerDeploy(config))
    }
  }

  def useToExitCode[R](name: String, r: Resource[IO, R]): IO[ExitCode] =
    r.attempt.use {
      case Right(_) => IO.never *> IO(ExitCode.Success)
      case Left(t)  => IO(logger.error(s"Failed to start $name", t)) *> IO(ExitCode.Error)
    }

  def streamingPartitionDeploy(config: Config): Resource[IO, PartitionsManager] = {
    val deploymentID = config.getString("raphtory.graph.id")
    val metricsPort  = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _                  <- Prometheus[IO](metricsPort)
      topicRepo          <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config)
      partitionIDManager <- Raphtory.makePartitionIdManager[IO](config, localDeployment = false, deploymentID)
      pm                 <- PartitionsManager
                              .streaming[IO](config, partitionIDManager, topicRepo, new Scheduler())
    } yield pm
  }

  def queryManagerDeploy(config: Config): Resource[IO, QueryManager] = {
    val metricsPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Prometheus[IO](metricsPort)
      topicRepo <- DistributedTopicRepository[IO](AkkaConnector.SeedMode, config)
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
