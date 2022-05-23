package com.raphtory.communication.topicRepositories

import com.raphtory.communication.TopicRepository
import com.raphtory.communication.connectors.PulsarConnector
import com.typesafe.config.Config

/** @DoNotDocument */
object PulsarTopicRepository {

  def apply(config: Config): TopicRepository = {
    val pulsarConnector = new PulsarConnector(config)
    new TopicRepository(pulsarConnector, config, Array(pulsarConnector))
  }
}
