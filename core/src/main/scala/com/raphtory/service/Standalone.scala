package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.Raphtory
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.components.cluster.ClusterManager
import com.raphtory.internals.components.cluster.RpcServer
import com.raphtory.internals.components.cluster.StandaloneMode
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.components.cluster.ClusterManager
import com.raphtory.internals.components.cluster.StandaloneMode
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.LocalHostAddressProvider
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

object Standalone extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val config   = Raphtory.getDefaultConfig()
    val headNode = for {
      repo               <- LocalTopicRepository[IO](config, None)
      partitionIdManager <- makeLocalIdManager[IO]
      _                  <- IngestionOrchestrator[IO](config, repo)
      _                  <- PartitionOrchestrator[IO](config, repo, partitionIdManager)
      _                  <- QueryOrchestrator[IO](config, repo)
      headNode           <- ClusterManager[IO](config, repo)
      sourceIDManager    <- makeLocalIdManager[IO]
      _                  <- RpcServer[IO](sourceIDManager, repo, config)
    } yield headNode
    headNode.useForever
  }
}
