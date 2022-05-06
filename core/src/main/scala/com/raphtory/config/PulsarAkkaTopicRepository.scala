package com.raphtory.config

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.deployment.PulsarAkkaGlobalFactory.actorSystem
import com.typesafe.config.Config

object PulsarAkkaTopicRepository {
  private lazy val actorSystem = ActorSystem(SpawnProtocol(), "spawner")

  def apply(config: Config): TopicRepository = {
    val akkaConnector   = new AkkaConnector(actorSystem)
    val pulsarConnector = new PulsarConnector(config)
    new TopicRepository(pulsarConnector, config) {
      override def jobOperationsConnector: Connector = akkaConnector
      override def jobStatusConnector: Connector     = akkaConnector
      override def queryPrepConnector: Connector     = akkaConnector
      override def endedQueriesConnector: Connector  = akkaConnector
      override def watermarkConnector: Connector     = akkaConnector
      override def rechecksConnector: Connector      = akkaConnector
    }
  }
}
