package com.raphtory.internals.components.querymanager

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.cluster.OrchestratorComponent
import com.typesafe.config.Config

class QueryOrchestrator(
    repo: TopicRepository,
    conf: Config
) extends OrchestratorComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting Query Service")

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String, clientID: String) =>
        establishService("Query Manager", graphID, clientID, repo, deployQueryService)
      case DestroyGraph(graphID, clientID, force)            => destroyGraph(graphID, clientID, force)
      case ClientDisconnected(graphID, clientID)             => clientDisconnected(graphID, clientID)
    }

}

object QueryOrchestrator {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, QueryOrchestrator] =
    Component.makeAndStart(
            topics,
            s"query-node",
            List(topics.clusterComms),
            new QueryOrchestrator(topics, conf)
    )
}
