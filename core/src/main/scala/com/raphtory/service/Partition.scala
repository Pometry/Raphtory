package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.partition.PartitionServiceImpl
import com.raphtory.internals.components.repositories.DistributedServiceRepository
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object Partition extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      topics <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      repo   <- DistributedServiceRepository[IO](topics, config)
      _      <- PartitionServiceImpl.makeN(repo, config)
    } yield ()
  }
}
