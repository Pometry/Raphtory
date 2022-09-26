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

    val config =
      if (args.nonEmpty) Raphtory.getDefaultConfig(Map("raphtory.deploy.id" -> args.head))
      else Raphtory.getDefaultConfig()

    val headNode = for {
      zkClient      <- ZookeeperConnector.getZkClient(config.getString("raphtory.zookeeper.address"))
      addressHandler = new ZKHostAddressProvider(zkClient, config, None)
      repo          <- DistributedTopicRepository[IO](AkkaConnector.SeedMode, config, addressHandler)
      idManager <- makeLocalIdManager[IO]
      headNode  <- ClusterManager[IO](config, repo, mode = ClusterMode, idManager)
      _         <- RpcServer[IO](idManager, repo, config)

    } yield headNode
    headNode.useForever

  }

}
