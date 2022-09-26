package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.connectors.ArrowFlightConnector
import com.raphtory.internals.communication.repositories.ArrowFlightRepository.signatureRegistry
import com.raphtory.internals.management.arrow.ArrowFlightHostAddressProvider
import com.typesafe.config.Config

private[raphtory] object LocalTopicRepository {

  def apply[IO[_]: Async](
      config: Config,
      addressProvider: ArrowFlightHostAddressProvider
  ): Resource[IO, TopicRepository] =
    config.getString("raphtory.communication.control") match {
      case "auto" | "akka" =>
        for {
          arrowFlightConnector <- ArrowFlightConnector[IO](config, signatureRegistry, addressProvider)
          akkaConnector        <- AkkaConnector[IO](AkkaConnector.StandaloneMode, config)
        } yield new TopicRepository(akkaConnector, akkaConnector, akkaConnector, config)
    }

}
