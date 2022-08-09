package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.cluster.OrchestratorComponent
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.typesafe.config.Config

class PartitionOrchestrator(
    conf: Config
) extends OrchestratorComponent(conf) {

  override private[raphtory] def run(): Unit =
    logger.info(s"Starting Partition Service for ${conf.getString("raphtory.deploy.id")}")

  override def handleMessage(msg: ClusterManagement): Unit =
    msg match {
      case EstablishGraph(graphID: String) => establishService("Partition Manager", graphID, deployPartitionService)
      case DestroyGraph(graphID)           => destroyGraph(graphID)
    }

}

object PartitionOrchestrator {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, PartitionOrchestrator] =
    Component.makeAndStart(
            topics,
            s"partition-node",
            List(topics.clusterComms),
            new PartitionOrchestrator(conf)
    )
}
