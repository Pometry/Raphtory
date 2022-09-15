package com.raphtory.internals.components.cluster

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.typesafe.config.Config

sealed trait DeploymentMode
case object StandaloneMode extends DeploymentMode
case object ClusterMode    extends DeploymentMode

class ClusterManager(
    conf: Config,
    topics: TopicRepository,
    mode: DeploymentMode
) extends OrchestratorComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting HeadNode for ${conf.getString("raphtory.deploy.id")}")

  private def forwardToCluster(msg: ClusterManagement) =
    topics.clusterComms(conf.getInt("raphtory.partitions.serverCount")).endPoint sendAsync msg

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String, clientID: String) =>
        mode match {
          case StandaloneMode =>
            deployStandaloneService(graphID, clientID, conf)
          case ClusterMode    =>
            logger.info(s"Forwarding deployment request for graph to cluster: '$graphID'")
            forwardToCluster(msg)
        }
      case DestroyGraph(graphID, clientID, force)            =>
        mode match {
          case StandaloneMode => destroyGraph(graphID, clientID, force)
          case ClusterMode    =>
            logger.info(s"Forwarding request to destroy graph to cluster: '$graphID'")
            forwardToCluster(msg)
        }
      case ClientDisconnected(graphID, clientID)             => clientDisconnected(graphID, clientID)

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
