package com.raphtory.deployment

import com.raphtory.config.Scheduler
import com.raphtory.config.TopicRepository
import com.typesafe.config.Config

trait GlobalFactory {
  def getScheduler: Scheduler
  def createTopicRepository(config: Config): TopicRepository
}
