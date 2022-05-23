package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.communication.TopicRepository
import com.raphtory.components.querymanager.Query
import com.raphtory.config.MonixScheduler
import com.typesafe.config.Config

class TemporalGraphConnection(
    query: Query,
    private val querySender: QuerySender,
    private val conf: Config,
    private val scheduler: MonixScheduler,
    private val topics: TopicRepository
) extends TemporalGraph(query, querySender, conf) {

  def disconnect(): Unit =
    scheduler.shutdown()

}
