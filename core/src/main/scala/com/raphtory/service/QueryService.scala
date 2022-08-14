package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ArrowFlightHostAddressProvider

object QueryService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val config =
      if (args.nonEmpty) Raphtory.getDefaultConfig(Map("raphtory.deploy.id" -> args.head))
      else Raphtory.getDefaultConfig()

    val service = for {
      zkClient      <- ZookeeperConnector.getZkClient(config.getString("raphtory.zookeeper.address"))
      addressHandler = new ArrowFlightHostAddressProvider(zkClient, config)
      repo          <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, addressHandler)
      service       <- QueryOrchestrator[IO](config, repo)
    } yield service
    service.useForever

  }
}
