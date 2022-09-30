package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.Raphtory.makePartitionIDManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.Prometheus
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

object PartitionService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val config  = Raphtory.getDefaultConfig()
    val service = for {
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      partitionIDManager <- makePartitionIDManager[IO](config)
      service            <- PartitionOrchestrator[IO](config, repo, partitionIDManager)
    } yield service
    service.useForever
  }
}
