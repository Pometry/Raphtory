package com.raphtory.internals.components.service

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.Raphtory.makePartitionIdManager
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.ingestion.IngestionExecutor
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.context.Service
import com.raphtory.internals.management.PartitionsManager
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository

import scala.collection.mutable

sealed trait DeploymentMode
case object StandaloneMode extends DeploymentMode
case object ClusterMode    extends DeploymentMode

class HeadNode(
    conf: Config,
    topics: TopicRepository,
    mode: DeploymentMode
) extends Component[ClusterManagement](conf) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting HeadNode for ${conf.getString("raphtory.deploy.id")}")
  private val graphDeployments               = mutable.Map[String, Service]()

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String) =>
        mode match {
          case StandaloneMode =>
            val graphConf = conf.withValue(
                    "raphtory.graph.id",
                    ConfigValueFactory.fromAnyRef(graphID)
            )
            val service   = deployStandaloneService(graphID, graphConf)
            graphDeployments.put(graphID, service)

          case ClusterMode    =>
        }
      case DestroyGraph(graphID)           =>
        graphDeployments.remove(graphID) match {
          case Some(service) =>
            logger.info(s"Destroying Graph $graphID")
            service.shutdown.unsafeRunSync()
          case None          => logger.warn(s"Graph $graphID requested for destruction, but did not exist")
        }
    }

  override private[raphtory] def stop(): Unit =
    graphDeployments.foreach {
      case (graphID, _) => graphDeployments.remove(graphID).foreach(_.shutdown.unsafeRunSync())
    }

  private def deployStandaloneService(graphID: String, graphConf: Config): Service = {
    val scheduler       = new Scheduler()
    val prometheusPort  = graphConf.getInt("raphtory.prometheus.metrics.port")
    val serviceResource = for {
      _                  <- Prometheus[IO](prometheusPort) //FIXME: need some sync because this thing does not stop
      partitionIdManager <- makePartitionIdManager[IO](graphConf, localDeployment = false, graphID)
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, graphConf)
      _                  <- PartitionsManager.streaming[IO](graphConf, partitionIdManager, repo, scheduler)
      _                  <- IngestionManager[IO](graphConf, repo)
      _                  <- QueryManager[IO](graphConf, repo)
    } yield new QuerySender(scheduler, repo, graphConf)

    val (client, shutdown) = serviceResource.allocated.unsafeRunSync()
    Service(client, graphID, shutdown)
  }
}

object HeadNode {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository,
      mode: DeploymentMode
  ): Resource[IO, HeadNode] =
    Component.makeAndStart(
            topics,
            s"head-node",
            List(topics.graphSetup),
            new HeadNode(conf, topics, mode)
    )
}
