package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.Raphtory
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.ingestion.IngestionServiceInstance
import com.raphtory.internals.components.repositories.DistributedServiceRepository

object Ingestion extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = Raphtory.getDefaultConfig()
    for {
      topics <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      repo   <- DistributedServiceRepository[IO](topics, config)
      _      <- IngestionServiceInstance[IO](repo, config)
    } yield ()
  }
}
