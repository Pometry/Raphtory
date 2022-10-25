package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.Raphtory
import com.raphtory.Raphtory.makePartitionIDManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionOrchestrator

object Partition extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = Raphtory.getDefaultConfig()
    for {
      repo               <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      partitionIDManager <- makePartitionIDManager[IO](config)
      _                  <- PartitionOrchestrator[IO](config, repo, partitionIDManager)
    } yield ()
  }
}
