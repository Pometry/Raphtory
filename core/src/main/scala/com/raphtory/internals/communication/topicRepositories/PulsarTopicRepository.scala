package com.raphtory.internals.communication.topicRepositories

import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.PulsarConnector
import com.typesafe.config.Config

/** @DoNotDocument */
object PulsarTopicRepository {

  def apply(config: Config): TopicRepository = {
    val pulsarConnector = new PulsarConnector(config)
    new TopicRepository(pulsarConnector, config, Array(pulsarConnector))
  }
}
