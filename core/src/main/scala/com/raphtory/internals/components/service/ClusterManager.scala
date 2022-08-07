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
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository

import scala.collection.mutable

sealed trait DeploymentMode
case object StandaloneMode extends DeploymentMode
case object ClusterMode    extends DeploymentMode

class ClusterManager(
    conf: Config,
    mode: DeploymentMode
) extends ServiceComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting HeadNode for ${conf.getString("raphtory.deploy.id")}")

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String) =>
        mode match {
          case StandaloneMode =>
            graphDeployments.get(graphID) match {
              case Some(value) => logger.info(s"New client connecting for graph: $graphID")
              case None        =>
                logger.info(s"Deploying new graph in standalone mode: $graphID")
                val graphConf = conf.withValue(
                        "raphtory.graph.id",
                        ConfigValueFactory.fromAnyRef(graphID)
                )
                val service   = deployStandaloneService(graphID, graphConf)
                graphDeployments.put(graphID, service)
            }
          case ClusterMode    =>
        }
      case DestroyGraph(graphID)           => destroyGraph(graphID)
    }
}

object ClusterManager {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository,
      mode: DeploymentMode
  ): Resource[IO, ClusterManager] =
    Component.makeAndStart(
            topics,
            s"head-node",
            List(topics.graphSetup),
            new ClusterManager(conf, mode)
    )
}
