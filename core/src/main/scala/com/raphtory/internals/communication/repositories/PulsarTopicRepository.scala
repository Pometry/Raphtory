package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.typesafe.config.Config

private[raphtory] object PulsarTopicRepository {

  def apply[IO[_]: Async](config: Config): Resource[IO, TopicRepository] =
    PulsarConnector(config).map(connector => new TopicRepository(connector, config, Array()))
}
