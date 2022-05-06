package com.raphtory.config

import com.typesafe.config.Config

object PulsarTopicRepository {

  def apply(config: Config): TopicRepository =
    new TopicRepository(new PulsarConnector(config), config)
}
