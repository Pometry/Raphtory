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
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator
import com.raphtory.internals.management.Prometheus

object IngestionService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    val config         = Raphtory.getDefaultConfig()
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")

    val service = for {
      _       <- Prometheus[IO](prometheusPort)
      repo    <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      service <- IngestionOrchestrator[IO](config, repo)
    } yield service
    service.useForever
  }
}
