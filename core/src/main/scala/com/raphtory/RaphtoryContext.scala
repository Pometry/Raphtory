package com.raphtory

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Sync
import com.oblac.nomen.Nomen
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.input.Source
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.management.ConfigHandler
import com.raphtory.internals.management.PartitionsManager
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.internals.management.id.ZookeeperIDManager
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.Raphtory.makePartitionIdManager
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

abstract class RaphtoryContext {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  case class Service(client: QuerySender, deploymentID: String, shutdown: IO[Unit], graphs: Set[String])

  def newGraph(name: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph

  protected def createName: String =
    Nomen.est().adjective().color().animal().get()

  def close()

  private[raphtory] def confBuilder(
      customConfig: Map[String, Any] = Map()
  ): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig()
  }

}

class LocalRaphtoryContext() extends RaphtoryContext {
  private var localService: Option[Service] = None

  def newGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph = {
    val ((client, config), shutdown) =
      deployLocalGraph[IO](graphID, customConfig).allocated.unsafeRunSync()
    new DeployedTemporalGraph(Query(graphID = graphID), client, config, shutdown)
  }

  def newIOGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()) =
    deployLocalGraph[IO](graphID, customConfig).map {
      case (qs, config) =>
        new DeployedTemporalGraph(Query(), qs, config, shutdown = IO.unit)
    }

  private def deployLocalService(graphID: String, config: Config): Service = {
    val scheduler       = new Scheduler()
    val prometheusPort  = config.getInt("raphtory.prometheus.metrics.port")
    val serviceResource = for {
      _                  <- Prometheus[IO](prometheusPort) //FIXME: need some sync because this thing does not stop
      topicRepo          <- LocalTopicRepository[IO](config)
      partitionIdManager <- makePartitionIdManager[IO](config, localDeployment = true, graphID)
      _                  <- PartitionsManager.streaming[IO](config, partitionIdManager, topicRepo, scheduler)
      _                  <- IngestionManager[IO](graphID, config, topicRepo)
      _                  <- QueryManager[IO](config, topicRepo)
    } yield new QuerySender(scheduler, topicRepo, config)

    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, graphID, shutdown, Set())
  }

  private def deployLocalGraph[IO[_]](
      graphID: String,
      customConfig: Map[String, Any]
  )(implicit
      IO: Async[IO]
  ): Resource[IO, (QuerySender, Config)] =
    Resource.make {
      val config = confBuilder(Map("raphtory.deploy.id" -> graphID))
      IO.delay {
        localService.synchronized {
          val service = localService match {
            case Some(service) =>
              logger.debug("There is an existing service, adding graph")
              service.copy(graphs = service.graphs + graphID)
            case None          =>
              logger.debug("No local service deployed, deploying it and adding the graph")
              deployLocalService(graphID, config).copy(graphs = Set(graphID))
          }
          localService = Some(service)
          val client  = service.client
          //client.submitGraph(sources, graphID)
          (client, config)
        }
      }
    } { service =>
      IO.delay {
        localService.synchronized {
          val remainingGraphs = localService.get.graphs - graphID
          localService = localService.map(service => service.copy(graphs = remainingGraphs))
          if (remainingGraphs.isEmpty) {
            localService.get.shutdown.unsafeRunSync()
            logger.debug("Last graph in the service was removed, shutting down")
          }
        }
      }
    }

  override def close(): Unit = ???
}
