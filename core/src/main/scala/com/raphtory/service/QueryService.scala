package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object QueryService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config  = ConfigBuilder().build().getConfig
    val service = for {
      repo    <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      service <- QueryOrchestrator[IO](config, repo)
    } yield service
    service.useForever
  }
}
