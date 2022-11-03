package com.raphtory.internals.components.querymanager

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.std.Dispatcher
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.components.cluster.OrchestratorComponent
import com.raphtory.protocol.PartitionService
import com.typesafe.config.Config

class QueryOrchestrator[F[_]: Async](
    dispatcher: Dispatcher[F],
    registry: ServiceRegistry[F],
    conf: Config
) extends OrchestratorComponent(dispatcher, registry, conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting Query Service")

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String, clientID: String) =>
        establishService("Query Manager", graphID, clientID, registry.topics, deployQueryService)
      case DestroyGraph(graphID, clientID, force)            => destroyGraph(graphID, clientID, force)
      case ClientDisconnected(graphID, clientID)             => clientDisconnected(graphID, clientID)
    }

}

object QueryOrchestrator {

  def apply[F[_]: Async](
      dispatcher: Dispatcher[F],
      conf: Config,
      registry: ServiceRegistry[F]
  ): Resource[F, QueryOrchestrator[F]] =
    Component.makeAndStart(
            registry.topics,
            s"query-node",
            List(registry.topics.clusterComms),
            new QueryOrchestrator(dispatcher, registry, conf)
    )
}
