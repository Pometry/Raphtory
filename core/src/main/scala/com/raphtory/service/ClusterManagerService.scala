package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object ClusterManagerService extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val config = ConfigBuilder.getDefaultConfig
    (for {
      service <- RaphtoryServiceBuilder.manager[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()).useForever
  }
}
