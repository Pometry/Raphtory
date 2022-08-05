package com.raphtory.internals.context

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import com.raphtory.Raphtory.makePartitionIdManager
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.management.PartitionsManager
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global

import scala.collection.mutable

class LocalRaphtoryContext() extends RaphtoryContext {
  private var localServices: mutable.Map[String, Service] = mutable.Map.empty[String, Service]

  def newGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph = {
    val ((client, config), shutdown) =
      deployLocalGraph[IO](graphID, customConfig).allocated.unsafeRunSync()
    new DeployedTemporalGraph(Query(), client, config, shutdown)
  }

  def newIOGraph(
      graphID: String = createName,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, DeployedTemporalGraph] =
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
    Service(client, graphID, shutdown)
  }

  private def deployLocalGraph[IO[_]](
      graphID: String,
      customConfig: Map[String, Any]
  )(implicit
      IO: Async[IO]
  ): Resource[IO, (QuerySender, Config)] =
    Resource.make {
      val config = confBuilder(Map("raphtory.deploy.id" -> graphID) ++ customConfig)
      IO.delay {
        localServices.synchronized {
          localServices.get(graphID) match {
            case Some(service) =>
              logger.info(s"Requested Graph $graphID already exists, returning service")
              (service.client, config)
            case None          =>
              logger.debug(s"Creating Service for $graphID")
              val service = deployLocalService(graphID, config)
              localServices += ((graphID, service))
              (service.client, config)

          }
        }
      }
    } { service =>
      IO.delay {
        localServices.synchronized {
          localServices.get(graphID) match {
            case Some(service) =>
              service.shutdown.unsafeRunSync()
              localServices.remove(graphID)
            case None          => //already closed?
          }
        }
      }
    }

  override def close(): Unit = {
    localServices.foreach(service => service._2.shutdown.unsafeRunSync())
    localServices = mutable.Map.empty[String, Service]
  }
}
