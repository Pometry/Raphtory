package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import cats.effect.std.Dispatcher
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryOrchestrator
import com.raphtory.internals.components.repositories.DistributedServiceRegistry
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Prometheus

object Query extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      dispatcher <- Dispatcher[IO]
      _          <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      topics     <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      registry   <- DistributedServiceRegistry[IO](topics, config)
      _          <- QueryOrchestrator[IO](dispatcher, config, registry)
    } yield ()
  }
}
