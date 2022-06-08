package com.raphtory.api.analysis.graphview

import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.MonixScheduler
import com.raphtory.internals.management.QuerySender
import com.typesafe.config.Config

/**  `TemporalGraphConnection` is a wrapper for the `TemporalGraph` class normally used to interact with Raphtory graphs.
  *  This is returned from the `connect` function on the [[com.raphtory.Raphtory]] Object and has an additional `disconnect()`
  *  function which allows the user to clean up the resources (scheduler/topic repo/connections etc.) used to connect to a deployment.
  *
  * @see
  *  [[com.raphtory.Raphtory]]
  *  [[DeployedTemporalGraph]]
  *  [[TemporalGraph]]
  */
class TemporalGraphConnection private[raphtory] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender,
    override private[api] val conf: Config,
    private val scheduler: MonixScheduler,
    private val topics: TopicRepository
) extends TemporalGraph(query, querySender, conf) {

  /** Disconnects the client from the deployed graph - cleans up all resources (scheduler/topic repo/connections etc.) used for the connection. */
  def disconnect(): Unit = {
    scheduler.shutdown()
    topics.shutdown()
  }

}
