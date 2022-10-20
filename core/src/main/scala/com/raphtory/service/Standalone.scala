package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.GraphConfig.ConfigBuilder

object Standalone extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    (for {
      config  <- Resource.pure(ConfigBuilder().getDefaultConfig)
      service <- RaphtoryServiceBuilder.standalone[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()).useForever
}
