package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

object PartitionService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val config =
      if (args.nonEmpty) Raphtory.getDefaultConfig(Map("raphtory.deploy.id" -> args.head))
      else Raphtory.getDefaultConfig()

    val service = for {
      zkClient      <- ZookeeperConnector.getZkClient(config.getString("raphtory.zookeeper.address"))
      allocator      = new RootAllocator
      arrowServer   <- ArrowFlightServer[IO](allocator)
      addressHandler = new ZKHostAddressProvider(zkClient, config, arrowServer, allocator)
      repo          <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, addressHandler)
      service       <- PartitionOrchestrator[IO](config, repo)
    } yield service
    service.useForever

  }
}
