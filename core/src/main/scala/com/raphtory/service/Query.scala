package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.components.querymanager.QueryServiceImpl
import com.raphtory.internals.components.registries.DistributedServiceRegistry
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Prometheus

object Query extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      _    <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      repo <- DistributedServiceRegistry[IO](config)
      _    <- QueryServiceImpl[IO](repo, config)
    } yield ()
  }
}
