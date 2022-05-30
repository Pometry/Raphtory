package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.communication.TopicRepository
import com.raphtory.components.querymanager.Query
import com.raphtory.config.MonixScheduler
import com.typesafe.config.Config

/**  `TemporalGraphConnection` is a wrapper for the `TemporalGraph` class normally used to interact with Raphtory graphs.
  *  This is returned from the `connect` function on the Raphtory Object and has an additional `disconnect()`
  *  function which allows the user to clean up the resources (scheduler/topic repo/connections etc.) used to connect to a deployment.
  *
  * @see
  *  [[com.raphtory.deployment.Raphtory]]
  *  [[com.raphtory.algorithms.api.DeployedTemporalGraph]]
  *  [[com.raphtory.algorithms.api.TemporalGraph]]
  */
class TemporalGraphConnection(
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender,
    private val conf: Config,
    private val scheduler: MonixScheduler,
    private val topics: TopicRepository
) extends TemporalGraph(query, querySender, conf) {

  /** Disconnects the client from the deployed graph - cleans up all resources (scheduler/topic repo/connections etc.) used for the connection. */
  def disconnect(): Unit = {
    scheduler.shutdown()
    topics.shutdown()
  }

}
