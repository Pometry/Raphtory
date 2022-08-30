package com.raphtory.internals.components.cluster

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.Raphtory.makeIdManager
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
  private case class Deployment(shutdown: IO[Unit], clients: mutable.Set[String])
  private val deployments      = mutable.Map[String, Deployment]()
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  protected def establishService(
      component: String,
      graphID: String,
      clientID: String,
      func: (String, String, Config) => Unit
  ): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          logger.info(s"$component for graph '$graphID' already exists, adding '$clientID' to registered users")
          deployment.clients += clientID
        case None             =>
          logger.info(s"Deploying new $component for graph '$graphID' - request by '$clientID' ")
          val graphConf = conf.withValue(
                  "raphtory.graph.id",
                  ConfigValueFactory.fromAnyRef(graphID)
          )
          func(graphID, clientID, graphConf)
      }
    }

  protected def destroyGraph(graphID: String, clientID: String, force: Boolean): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          deployment.clients.remove(clientID)
          if (force || deployment.clients.isEmpty) {
            logger.info(s"Last client '$clientID' disconnected. Destroying Graph '$graphID'")
            deployments.remove(graphID)
            deployment.shutdown.unsafeRunSync()
          }
          else
            logger.info(s"Client '$clientID' disconnected from Graph '$graphID'")
        case None             => logger.warn(s"Graph '$graphID' requested for destruction by '$clientID', but did not exist")
      }
    }

  protected def clientDisconnected(graphID: String, clientID: String): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          deployment.clients.remove(clientID)
          logger.info(s"Client '$clientID' disconnected from Graph '$graphID'")
        case None             => logger.warn(s"'$clientID' disconnected from Graph '$graphID', but did not exist")
      }
    }

  override private[raphtory] def stop(): Unit =
    deployments.foreach {
      case (_, deployment) => deployment.shutdown.unsafeRunSync()
    }

  protected def deployStandaloneService(graphID: String, clientID: String, graphConf: Config): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          logger.info(s"New client '$clientID' connecting for graph: '$graphID'")
          deployment.clients += clientID
        case None             =>
          logger.info(s"Deploying new graph '$graphID' in standalone mode, requested by '$clientID' ")
          val graphConf       = conf.withValue(
                  "raphtory.graph.id",
                  ConfigValueFactory.fromAnyRef(graphID)
          )
          val scheduler       = new Scheduler()
          val serviceResource = for {
            partitionIdManager <- makeIdManager[IO](graphConf, localDeployment = false, graphID, forPartitions = true)
            sourceIdManager    <- makeIdManager[IO](graphConf, localDeployment = false, graphID, forPartitions = false)
            repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
            _                  <- PartitionOrchestrator.spawn[IO](graphConf, partitionIdManager, repo, scheduler)
            _                  <- IngestionManager[IO](graphConf, repo, sourceIdManager)
            _                  <- QueryManager[IO](graphConf, repo)
          } yield ()
          val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
          deployments += ((graphID, Deployment(shutdown, clients = mutable.Set(clientID))))
      }
    }

  protected def deployPartitionService(graphID: String, clientID: String, graphConf: Config): Unit =
    deployments.synchronized {
      val scheduler       = new Scheduler()
      val serviceResource = for {
        partitionIdManager <- makeIdManager[IO](graphConf, localDeployment = false, graphID, forPartitions = true)
        repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
        _                  <- PartitionOrchestrator.spawn[IO](graphConf, partitionIdManager, repo, scheduler)
      } yield ()
      val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
      deployments += ((graphID, Deployment(shutdown, clients = mutable.Set(clientID))))
    }

  protected def deployIngestionService(graphID: String, clientID: String, graphConf: Config): Unit =
    deployments.synchronized {
      val serviceResource = for {
        repo            <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
        sourceIdManager <- makeIdManager[IO](graphConf, localDeployment = false, graphID, forPartitions = false)
        _               <- IngestionManager[IO](graphConf, repo, sourceIdManager)
      } yield ()
      val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
      deployments += ((graphID, Deployment(shutdown, clients = mutable.Set(clientID))))
    }

  protected def deployQueryService(graphID: String, clientID: String, graphConf: Config): Unit =
    deployments.synchronized {
      val serviceResource = for {
        repo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
        _    <- QueryManager[IO](graphConf, repo)
      } yield ()
      val (_, shutdown)   = serviceResource.allocated.unsafeRunSync()
      deployments += ((graphID, Deployment(shutdown, clients = mutable.Set(clientID))))
    }

}
