package com.raphtory.deployment

import akka.actor.typed.ActorSystem
import akka.actor.typed.SpawnProtocol
import com.raphtory.config.AkkaConnector
import com.raphtory.config.AkkaScheduler
import com.raphtory.config.Connector
import com.raphtory.config.PulsarConnector
import com.raphtory.config.Scheduler
import com.raphtory.config.TopicRepository
import com.typesafe.config.Config

object PulsarAkkaGlobalFactory extends GlobalFactory {
  private val actorSystem          = ActorSystem(SpawnProtocol(), "spawner")
  private val scheduler: Scheduler = new AkkaScheduler(actorSystem)
  private val akkaConnector        = new AkkaConnector(actorSystem)

  override def getScheduler: Scheduler = scheduler

  override def createTopicRepository(config: Config): TopicRepository =
    new TopicRepository(new PulsarConnector(config), config) {
      override def jobOperationsConnector: Connector = akkaConnector
      override def jobStatusConnector: Connector     = akkaConnector
      override def queryPrepConnector: Connector     = akkaConnector
      override def endedQueriesConnector: Connector  = akkaConnector
      override def watermarkConnector: Connector     = akkaConnector
      override def rechecksConnector: Connector      = akkaConnector
    }
}
