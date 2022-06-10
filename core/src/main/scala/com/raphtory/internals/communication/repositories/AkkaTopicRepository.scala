package com.raphtory.internals.communication.repositories

import cats.effect.Resource
import cats.effect.Sync
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.typesafe.config.Config

/** This object only exists for testing purposes -- no more deployments are fully akka */
private[raphtory] object AkkaTopicRepository {

  def apply[IO[_]: Sync](config: Config): Resource[IO, TopicRepository] = {
    val connectorResource = AkkaConnector(AkkaConnector.StandaloneMode, config)
    connectorResource.map(akkaConnector => new TopicRepository(akkaConnector, config, Array()))
  }
}
