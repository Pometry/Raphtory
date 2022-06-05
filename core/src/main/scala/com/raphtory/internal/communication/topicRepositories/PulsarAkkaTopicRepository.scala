package com.raphtory.internal.communication.topicRepositories

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.internal.communication.connectors.AkkaConnector
import com.raphtory.internal.communication.connectors.PulsarConnector
import com.raphtory.internal.communication.Connector
import com.raphtory.internal.communication.TopicRepository
import com.typesafe.config.Config

/** @DoNotDocument */
object PulsarAkkaTopicRepository {

  def apply(config: Config): TopicRepository = {
    val actorSystem     = ActorSystem(SpawnProtocol(), "spawner")
    val akkaConnector   = new AkkaConnector(actorSystem)
    val pulsarConnector = new PulsarConnector(config)
    new TopicRepository(pulsarConnector, config, Array(akkaConnector, pulsarConnector)) {
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
