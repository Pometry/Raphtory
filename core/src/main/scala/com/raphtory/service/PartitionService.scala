package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.makePartitionIDManager

object PartitionService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config  = ConfigBuilder.getDefaultConfig
    val service = for {
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      partitionIDManager <- makePartitionIDManager[IO](config)
      service            <- PartitionOrchestrator[IO](config, repo, partitionIDManager)
    } yield service
    service.useForever
  }
}
