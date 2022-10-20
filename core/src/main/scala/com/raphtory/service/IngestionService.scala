package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object IngestionService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config  = ConfigBuilder().getDefaultConfig
    val service = for {
      repo          <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      service       <- IngestionOrchestrator[IO](config, repo)
    } yield service
    service.useForever
  }
}
