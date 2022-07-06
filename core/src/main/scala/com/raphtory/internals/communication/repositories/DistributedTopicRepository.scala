package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.typesafe.config.Config

private[raphtory] object DistributedTopicRepository {

  def apply[IO[_]: Async](akkaMode: AkkaConnector.Mode, config: Config): Resource[IO, TopicRepository] =
    config.getString("raphtory.communication.control") match {
      case "akka"            =>
        for {
          pulsarConnector <- PulsarConnector[IO](config)
          akkaConnector   <- AkkaConnector[IO](akkaMode, config)
        } yield TopicRepository(akkaConnector, pulsarConnector, config)
      case "auto" | "pulsar" => PulsarConnector[IO](config).map(connector => TopicRepository(connector, config))
    }
}
