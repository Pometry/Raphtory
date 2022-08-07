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

class IngestionService(
    conf: Config
) extends ServiceComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting Ingestion Service for ${conf.getString("raphtory.deploy.id")}")

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String) => establishService("Ingestion Manager", graphID, deployIngestionService)
      case DestroyGraph(graphID)           => destroyGraph(graphID)
    }
}

object IngestionService {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, IngestionService] =
    Component.makeAndStart(
            topics,
            s"ingestion-node",
            List(topics.graphSetup),
            new IngestionService(conf)
    )
}
