package com.raphtory.communication.topicRepositories

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.communication.TopicRepository
import com.raphtory.communication.connectors.AkkaConnector
import com.typesafe.config.Config

/** @DoNotDocument This object only exists for testing purposes -- no more deployments are fully akka */
object AkkaTopicRepository {
  private lazy val actorSystem = ActorSystem(SpawnProtocol(), "spawner")

  def apply(config: Config): TopicRepository = {
    val akkaConnector = new AkkaConnector(actorSystem)
    new TopicRepository(akkaConnector, config)
  }
}
