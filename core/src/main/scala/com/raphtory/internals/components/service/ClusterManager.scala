package com.raphtory.internals.components.service

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory

sealed trait DeploymentMode
case object StandaloneMode extends DeploymentMode
case object ClusterMode    extends DeploymentMode

class ClusterManager(
    conf: Config,
    topics: TopicRepository,
    mode: DeploymentMode
) extends ServiceComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting HeadNode for ${conf.getString("raphtory.deploy.id")}")

  private def forwardToCluster(msg: ClusterManagement) =
    topics.clusterComms.endPoint sendAsync msg

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
            logger.info(s"Forwarding deployment request for graph to cluster: $graphID")
            forwardToCluster(msg)
        }
      case DestroyGraph(graphID)           =>
        mode match {
          case StandaloneMode => destroyGraph(graphID)
          case ClusterMode    =>
            logger.info(s"Forwarding request to destroy graph to cluster: $graphID")
            forwardToCluster(msg)
        }
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
            new ClusterManager(conf, topics, mode)
    )
}
