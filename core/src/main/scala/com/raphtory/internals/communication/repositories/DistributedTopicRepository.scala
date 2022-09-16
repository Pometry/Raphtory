package com.raphtory.internals.communication.repositories

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.typesafe.config.Config

private[raphtory] object DistributedTopicRepository {

  def apply[IO[_]: Async](akkaMode: AkkaConnector.Mode, config: Config): Resource[IO, TopicRepository] =
    config.getString("raphtory.communication.control") match {
      case "auto" | "akka" =>
        for {
          akkaConnector <- AkkaConnector[IO](akkaMode, config)
        } yield TopicRepository(akkaConnector, akkaConnector, config)
    }
}
