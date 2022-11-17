package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.QueryServiceImpl
import com.raphtory.internals.components.repositories.DistributedServiceRegistry
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Prometheus

object Query extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      _      <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      topics <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config, None)
      repo   <- DistributedServiceRegistry[IO](topics, config)
      _      <- QueryServiceImpl[IO](repo, config)
    } yield ()
  }
}
