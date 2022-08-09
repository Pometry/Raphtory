package com.raphtory.internals.components.cluster

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.Raphtory.makePartitionIdManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.context.Service
import com.raphtory.internals.management.PartitionsManager
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

abstract class OrchestratorComponent(conf: Config) extends Component[ClusterManagement](conf) {
  protected val graphDeployments = mutable.Map[String, Service]()
  protected val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))

  protected def establishService(component: String, graphID: String, func: (String, Config) => Service): Unit =
    graphDeployments.get(graphID) match {
      case Some(value) =>
      case None        =>
        logger.info(s"Deploying new $component for graph: $graphID")
        val graphConf = conf.withValue(
                "raphtory.graph.id",
                ConfigValueFactory.fromAnyRef(graphID)
        )
        val service   = func(graphID, graphConf)
        graphDeployments.put(graphID, service)
    }

  protected def destroyGraph(graphID: String): Unit =
    graphDeployments.remove(graphID) match {
      case Some(service) =>
        logger.info(s"Destroying Graph $graphID")
        service.shutdown.unsafeRunSync()
      case None          => logger.warn(s"Graph $graphID requested for destruction, but did not exist")
    }

  override private[raphtory] def stop(): Unit =
    graphDeployments.foreach {
      case (graphID, _) => graphDeployments.remove(graphID).foreach(_.shutdown.unsafeRunSync())
    }

  protected def deployStandaloneService(graphID: String, graphConf: Config): Service = {
    val scheduler       = new Scheduler()
    val serviceResource = for {
      partitionIdManager <- makePartitionIdManager[IO](graphConf, localDeployment = false, graphID)
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _                  <- PartitionsManager.streaming[IO](graphConf, partitionIdManager, repo, scheduler)
      _                  <- IngestionManager[IO](graphConf, repo)
      _                  <- QueryManager[IO](graphConf, repo)
    } yield new QuerySender(scheduler, repo, graphConf)

    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, graphID, conf, shutdown)
  }

  protected def deployPartitionService(graphID: String, graphConf: Config): Service = {
    val scheduler       = new Scheduler()
    val serviceResource = for {
      partitionIdManager <- makePartitionIdManager[IO](graphConf, localDeployment = false, graphID)
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _                  <- PartitionsManager.streaming[IO](graphConf, partitionIdManager, repo, scheduler)
    } yield new QuerySender(scheduler, repo, graphConf)

    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, graphID, conf, shutdown)
  }

  protected def deployIngestionService(graphID: String, graphConf: Config): Service = {
    val serviceResource = for {
      repo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _    <- IngestionManager[IO](graphConf, repo)
    } yield new QuerySender(new Scheduler(), repo, graphConf)

    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, graphID, conf, shutdown)
  }

  protected def deployQueryService(graphID: String, graphConf: Config): Service = {
    val serviceResource    = for {
      repo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _    <- QueryManager[IO](graphConf, repo)
    } yield new QuerySender(new Scheduler(), repo, graphConf)
    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, graphID, conf, shutdown)
  }

}
