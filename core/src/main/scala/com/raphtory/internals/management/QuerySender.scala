package com.raphtory.internals.management

import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.Query

import scala.util.Random

private[raphtory] class QuerySender(
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
