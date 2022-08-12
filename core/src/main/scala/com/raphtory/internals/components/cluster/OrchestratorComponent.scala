package com.raphtory.internals.components.cluster

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.Raphtory.makePartitionIdManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

abstract class OrchestratorComponent(conf: Config) extends Component[ClusterManagement](conf) {
  private val deployments      = mutable.Map[String, IO[Unit]]()
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  protected def establishService(component: String, graphID: String, func: (String, Config) => Unit): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(_) => logger.info(s"$component for graph $graphID already exists")
        case None    =>
          logger.info(s"Deploying new $component for graph: $graphID")
          val graphConf = conf.withValue(
                  "raphtory.graph.id",
                  ConfigValueFactory.fromAnyRef(graphID)
          )
          func(graphID, graphConf)
      }
    }

  protected def destroyGraph(graphID: String): Unit =
    deployments.synchronized {
      deployments.remove(graphID) match {
        case Some(deployment) =>
          logger.info(s"Destroying Graph $graphID")
          deployment.unsafeRunSync()
        case None             => logger.warn(s"Graph $graphID requested for destruction, but did not exist")
      }
    }

  override private[raphtory] def stop(): Unit =
    deployments.foreach {
      case (_, deployment) => deployment.unsafeRunSync()
    }

  protected def deployStandaloneService(graphID: String, graphConf: Config): Unit =
    deployments.get(graphID) match {
      case Some(value) => logger.info(s"New client connecting for graph: $graphID")
      case None        =>
        logger.info(s"Deploying new graph in standalone mode: $graphID")
        val graphConf       = conf.withValue(
                "raphtory.graph.id",
                ConfigValueFactory.fromAnyRef(graphID)
        )
        val scheduler       = new Scheduler()
        val serviceResource = for {
          partitionIdManager <- makePartitionIdManager[IO](graphConf, localDeployment = false, graphID)
          repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
          _                  <- PartitionOrchestrator.spawn[IO](graphConf, partitionIdManager, repo, scheduler)
          _                  <- IngestionManager[IO](graphConf, repo)
          _                  <- QueryManager[IO](graphConf, repo)
        } yield ()
        val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
        deployments.synchronized {
          deployments += ((graphID, shutdown))
        }
    }

  protected def deployPartitionService(graphID: String, graphConf: Config): Unit = {
    val scheduler       = new Scheduler()
    val serviceResource = for {
      partitionIdManager <- makePartitionIdManager[IO](graphConf, localDeployment = false, graphID)
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _                  <- PartitionOrchestrator.spawn[IO](graphConf, partitionIdManager, repo, scheduler)
    } yield ()
    val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
    deployments.synchronized {
      deployments += ((graphID, shutdown))
    }
  }

  protected def deployIngestionService(graphID: String, graphConf: Config): Unit = {
    val serviceResource = for {
      repo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _    <- IngestionManager[IO](graphConf, repo)
    } yield ()
    val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
    deployments.synchronized {
      deployments += ((graphID, shutdown))
    }
  }

  protected def deployQueryService(graphID: String, graphConf: Config): Unit = {
    val serviceResource = for {
      repo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _    <- QueryManager[IO](graphConf, repo)
    } yield ()
    val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
    deployments.synchronized {
      deployments += ((graphID, shutdown))
    }
  }

}
