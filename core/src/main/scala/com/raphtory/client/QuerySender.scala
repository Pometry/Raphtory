package com.raphtory.client

import com.raphtory.communication.TopicRepository
import com.raphtory.components.querymanager.Query
import com.raphtory.components.querytracker.QueryProgressTracker
import com.raphtory.config.ComponentFactory
import com.raphtory.config.Scheduler

import scala.util.Random

/** @DoNotDocument */
class QuerySender(
    private val componentFactory: ComponentFactory,
    private val scheduler: Scheduler,
    private val topics: TopicRepository
) {

  def submit(query: Query, customJobName: String = ""): QueryProgressTracker = {
    val jobName     = if (customJobName.nonEmpty) customJobName else getDefaultName(query)
    val jobID       = jobName + "_" + Random.nextLong().abs
    val outputQuery = query.copy(name = jobID)
    topics.submissions.endPoint sendAsync outputQuery
    componentFactory.queryProgressTracker(jobID, scheduler)
  }

  private def getDefaultName(query: Query): String =
    if (query.name.nonEmpty) query.name else query.hashCode().abs.toString
}
