package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.cluster.ClusterManager
import com.raphtory.internals.components.cluster.ClusterMode

object ClusterManagerService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val config =
      if (args.nonEmpty) Raphtory.getDefaultConfig(Map("raphtory.deploy.id" -> args.head))
      else Raphtory.getDefaultConfig()

    val headNode = for {
      repo     <- DistributedTopicRepository[IO](AkkaConnector.SeedMode, config)
      headNode <- ClusterManager[IO](config, repo, mode = ClusterMode)
    } yield headNode
    headNode.useForever

  }

}
