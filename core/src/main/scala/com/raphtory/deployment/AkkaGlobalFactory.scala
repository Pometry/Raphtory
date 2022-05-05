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
import com.typesafe.config.ConfigFactory

import collection.JavaConverters._

object AkkaGlobalFactory extends GlobalFactory {
  private val actorSystem          = ActorSystem(SpawnProtocol(), "spawner")
  private val scheduler: Scheduler = new AkkaScheduler(actorSystem)

  override def getScheduler: Scheduler = scheduler

  override def createTopicRepository(config: Config): TopicRepository = {
    val akkaConnector = new AkkaConnector(actorSystem)
    new TopicRepository(akkaConnector, config)
  }
}
