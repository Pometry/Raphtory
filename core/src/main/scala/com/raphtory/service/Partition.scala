package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Prometheus
import com.raphtory.makePartitionIDManager

object Partition extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      _                  <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      partitionIDManager <- makePartitionIDManager[IO](config)
      _                  <- PartitionOrchestrator[IO](config, repo, partitionIDManager)
    } yield ()
  }
}
