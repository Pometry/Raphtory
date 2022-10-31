package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.Raphtory
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.components.ingestion.IngestionOrchestrator
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.components.DefaultRaphtoryService
import cats.effect.ResourceApp
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object Standalone extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      service <- RaphtoryServiceBuilder.standalone[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()
  }

}
