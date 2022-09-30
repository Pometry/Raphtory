package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.Raphtory
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

object QueryService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config  = Raphtory.getDefaultConfig()
    val service = for {
      repo    <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      service <- QueryOrchestrator[IO](config, repo)
    } yield service
    service.useForever
  }
}
