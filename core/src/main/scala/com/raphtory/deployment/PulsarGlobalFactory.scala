package com.raphtory.deployment

import com.raphtory.config.MonixScheduler
import com.raphtory.config.PulsarConnector
import com.raphtory.config.Scheduler
import com.raphtory.config.TopicRepository
import com.typesafe.config.Config

object PulsarGlobalFactory extends GlobalFactory {
  private lazy val scheduler = new MonixScheduler()

  override def getScheduler: Scheduler = scheduler

  override def createTopicRepository(config: Config): TopicRepository =
    new TopicRepository(new PulsarConnector(config), config)
}
