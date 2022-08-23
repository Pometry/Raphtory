package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.cluster.OrchestratorComponent
import com.raphtory.internals.components.querymanager.ClientDisconnected
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.typesafe.config.Config

class IngestionOrchestrator(
    conf: Config
) extends OrchestratorComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting Ingestion Service for ${conf.getString("raphtory.deploy.id")}")

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String, clientID: String) =>
        establishService("Query Manager", graphID, clientID, deployQueryService)
      case DestroyGraph(graphID, clientID, force)            => destroyGraph(graphID, clientID, force)
      case ClientDisconnected(graphID, clientID)             => clientDisconnected(graphID, clientID)
    }
}

object IngestionOrchestrator {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, IngestionOrchestrator] =
    Component.makeAndStart(
            topics,
            s"ingestion-node",
            List(topics.clusterComms),
            new IngestionOrchestrator(conf)
    )
}
