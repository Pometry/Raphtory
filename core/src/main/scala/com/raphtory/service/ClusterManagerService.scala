package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.cluster.ClusterManager
import com.raphtory.internals.components.cluster.ClusterMode
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import com.raphtory.internals.components.cluster.RpcServer

object ClusterManagerService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config   = Raphtory.getDefaultConfig()
    val headNode = for {
      repo      <- DistributedTopicRepository[IO](AkkaConnector.SeedMode, config, None)
      idManager <- makeLocalIdManager[IO]
      headNode  <- ClusterManager[IO](config, repo)
      _         <- RpcServer[IO](idManager, repo, config)
    } yield headNode
    headNode.useForever
  }
}
