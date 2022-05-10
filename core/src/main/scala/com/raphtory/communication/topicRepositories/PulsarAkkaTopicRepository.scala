package com.raphtory.communication.topicRepositories

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.communication.Connector
import com.raphtory.communication.TopicRepository
import com.raphtory.communication.connectors.AkkaConnector
import com.raphtory.communication.connectors.PulsarConnector
import com.typesafe.config.Config

/** @DoNotDocument */
object PulsarAkkaTopicRepository {
  private lazy val actorSystem = ActorSystem(SpawnProtocol(), "spawner")

  def apply(config: Config): TopicRepository = {
    val akkaConnector   = new AkkaConnector(actorSystem)
    val pulsarConnector = new PulsarConnector(config)
    new TopicRepository(pulsarConnector, config) {
      override def jobOperationsConnector: Connector    = akkaConnector
      override def jobStatusConnector: Connector        = akkaConnector
      override def queryPrepConnector: Connector        = akkaConnector
      override def completedQueriesConnector: Connector = akkaConnector
      override def watermarkConnector: Connector        = akkaConnector
      override def rechecksConnector: Connector         = akkaConnector
      override def queryTrackConnector: Connector       = akkaConnector
      override def submissionsConnector: Connector      = akkaConnector
    }
  }
}
