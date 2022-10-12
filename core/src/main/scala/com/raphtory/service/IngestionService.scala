package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management._
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

object IngestionService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config  = ConfigBuilder().build().getConfig
    val service = for {
      repo          <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      service       <- IngestionOrchestrator[IO](config, repo)
    } yield service
    service.useForever
  }
}
