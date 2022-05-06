package com.raphtory.config

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.typesafe.config.Config

object AkkaTopicRepository {
  private lazy val actorSystem = ActorSystem(SpawnProtocol(), "spawner")

  def apply(config: Config): TopicRepository = {
    val akkaConnector = new AkkaConnector(actorSystem)
    new TopicRepository(akkaConnector, config)
  }
}
