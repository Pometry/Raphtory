package com.raphtory.internals.management.client

import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querytracker.QueryProgressTracker
import com.raphtory.internals.management.ComponentFactory
import com.raphtory.internals.management.MonixScheduler

import scala.util.Random

/** @note DoNotDocument */
class QuerySender(
    private val componentFactory: ComponentFactory,
    private val scheduler: MonixScheduler,
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
