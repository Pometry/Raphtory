package com.raphtory.internals.communication.repositories

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.typesafe.config.Config

/** @DoNotDocument This object only exists for testing purposes -- no more deployments are fully akka */
object AkkaTopicRepository {

  def apply(config: Config): TopicRepository = {
    val actorSystem   = ActorSystem(SpawnProtocol(), "spawner")
    val akkaConnector = new AkkaConnector(actorSystem)
    new TopicRepository(akkaConnector, config, Array(akkaConnector))
  }
}
