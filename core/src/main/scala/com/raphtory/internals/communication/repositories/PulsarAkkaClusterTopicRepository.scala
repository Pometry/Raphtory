package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.Connector
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.typesafe.config.Config

object PulsarAkkaClusterTopicRepository {

  def apply[IO[_]: Async](config: Config, seed: Boolean = false): Resource[IO, TopicRepository] = {
    val akkaMode = if (seed) AkkaConnector.SeedMode else AkkaConnector.ClientMode
    for {
      akkaConnector   <- AkkaConnector[IO](akkaMode, config)
      pulsarConnector <- PulsarConnector[IO](config)
    } yield new TopicRepository(pulsarConnector, config, Array(akkaConnector, pulsarConnector)) {
      override def jobOperationsConnector: Connector      = akkaConnector
      override def jobStatusConnector: Connector          = akkaConnector
      override def queryPrepConnector: Connector          = akkaConnector
      override def completedQueriesConnector: Connector   = akkaConnector
      override def watermarkConnector: Connector          = akkaConnector
      override def rechecksConnector: Connector           = akkaConnector
      override def queryTrackConnector: Connector         = akkaConnector
      override def submissionsConnector: Connector        = akkaConnector
      override def vertexMessagesSyncConnector: Connector = akkaConnector
    }
  }
}
