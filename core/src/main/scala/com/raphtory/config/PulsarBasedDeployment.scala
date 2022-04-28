package com.raphtory.config

import com.typesafe.config.Config

object PulsarBasedDeployment {

  def apply(config: Config): (Scheduler, TopicRepository) = {
    val scheduler: Scheduler    = new MonixScheduler()
    val topics: TopicRepository = new TopicRepository(new PulsarConnector(config), config)
    (scheduler, topics)
  }
}
