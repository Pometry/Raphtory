package com.raphtory.service

import cats.effect.ExitCode
import cats.effect.IO
import cats.effect.IOApp
import cats.effect.Resource
import com.raphtory.Raphtory
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.arrow.ZKHostAddressProvider

object ClusterManagerService extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    (for {
      config  <- Resource.pure(Raphtory.getDefaultConfig())
      service <- RaphtoryServiceBuilder.manager[IO](config)
      _       <- RaphtoryServiceBuilder.server(service, config)
    } yield ()).useForever
}
