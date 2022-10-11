package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.Raphtory
import com.raphtory.internals.components.RaphtoryServiceBuilder

object Standalone extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    (for {
      config  <- Resource.pure(Raphtory.getDefaultConfig())
      service <- RaphtoryServiceBuilder.standalone[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()).useForever
}
