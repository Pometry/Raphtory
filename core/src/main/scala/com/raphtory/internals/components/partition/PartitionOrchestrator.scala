package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import cats.implicits.toTraverseOps
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.cluster.OrchestratorComponent
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.DestroyGraph
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.id.IDManager
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

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

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def nextId[IO[_]: Async](partitionIDManager: IDManager): IO[Int] =
    Async[IO].blocking {
      partitionIDManager.getNextAvailableID() match {
        case Some(id) => id
        case None     =>
          throw new Exception(s"Failed to retrieve Partition ID")
      }
    }

  def spawn[IO[_]: Spawn](
      config: Config,
      partitionIDManager: IDManager,
      topics: TopicRepository,
      scheduler: Scheduler
  )(implicit
      IO: Async[IO]
  ): Resource[IO, List[PartitionManager]] = {

    val graphID         = config.getString("raphtory.graph.id")
    val totalPartitions = config.getInt("raphtory.partitions.countPerServer")

    logger.info(s"Creating '$totalPartitions' Partition Managers for $graphID.")

    (0 until totalPartitions)
      .map { i =>
        for {
          partitionId <- Resource.eval(nextId(partitionIDManager))
          partition   <- PartitionManager(graphID, partitionId, scheduler, config, topics)
        } yield partition
      }
      .toList
      .sequence
  }

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
