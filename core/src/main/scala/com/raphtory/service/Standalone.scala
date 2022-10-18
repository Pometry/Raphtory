package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
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
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.LocalHostAddressProvider
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import com.raphtory.protocol.RaphtoryService
import com.typesafe.config.Config
import higherkindness.mu.rpc.server.AddService
import higherkindness.mu.rpc.server.GrpcServer
import org.apache.arrow.memory.RootAllocator

object Standalone extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    (for {
      config  <- Resource.pure(Raphtory.getDefaultConfig())
      service <- RaphtoryServiceBuilder.standalone[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()).useForever
}
