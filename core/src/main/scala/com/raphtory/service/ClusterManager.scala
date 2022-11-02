package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.management.Prometheus

object ClusterManager extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] = {
    val config = ConfigBuilder.getDefaultConfig
    for {
      _       <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      service <- RaphtoryServiceBuilder.manager[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()
  }
}
