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
import com.raphtory.internals.management.id.LocalIDManager

object Standalone extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config   = Raphtory.getDefaultConfig()
    val headNode = for {
      repo               <- LocalTopicRepository[IO](config)
      sourceIDManager    <- makeLocalIdManager[IO]
      partitionIdManager <- makeLocalIdManager[IO]
      headNode           <- ClusterManager[IO](config, repo, mode = StandaloneMode, partitionIdManager)
      _                  <- RpcServer[IO](sourceIDManager, repo, config)
    } yield headNode
    headNode.useForever
  }
}
