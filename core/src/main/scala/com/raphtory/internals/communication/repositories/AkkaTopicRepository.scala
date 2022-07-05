package com.raphtory.internals.communication.repositories

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import cats.effect.Resource
import cats.effect.Sync
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.typesafe.config.Config

/** This object only exists for testing purposes -- no more deployments are fully akka */
private[raphtory] object AkkaTopicRepository {

  def apply[IO[_]: Sync](config: Config): Resource[IO, TopicRepository] =
    makeConnector
      .map(akkaConnector => new TopicRepository(akkaConnector, config, Array()))

  def makeConnector[IO[_]: Sync]: Resource[IO, AkkaConnector] =
    Resource
      .make(Sync[IO].delay(ActorSystem(SpawnProtocol(), "spawner")))(system => Sync[IO].delay(system.terminate()))
      .map(new AkkaConnector(_))
}
