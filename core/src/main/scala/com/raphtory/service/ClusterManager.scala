package com.raphtory.service

import cats.effect.IO
import cats.effect.Resource
import cats.effect.ResourceApp
import com.raphtory.Raphtory
import com.raphtory.internals.components.RaphtoryServiceBuilder

object ClusterManager extends ResourceApp.Forever {

  def run(args: List[String]): Resource[IO, Unit] =
    for {
      config  <- Resource.pure(Raphtory.getDefaultConfig())
      service <- RaphtoryServiceBuilder.manager[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()
}
