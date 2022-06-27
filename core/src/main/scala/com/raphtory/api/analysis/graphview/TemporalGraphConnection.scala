package com.raphtory.api.analysis.graphview

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.QuerySender
import com.typesafe.config.Config

/**  `TemporalGraphConnection` is a wrapper for the `TemporalGraph` class normally used to interact with Raphtory graphs.
  *  This is returned from the `connect` function on the [[com.raphtory.Raphtory]] Object and has an additional `disconnect()`
  *  function which allows the user to clean up the resources (scheduler/topic repo/connections etc.) used to connect to a deployment.
  *
  * @see
  *  [[com.raphtory.Raphtory Raphtory]]
  *  [[DeployedTemporalGraph]]
  *  [[TemporalGraph]]
  */
class TemporalGraphConnection private[raphtory] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender,
    override private[api] val conf: Config,
    private val shutdown: IO[Unit]
) extends TemporalGraph(query, querySender, conf)
        with AutoCloseable {
  override def close(): Unit = shutdown.unsafeRunSync()

  /**
    * For compatibility with Python API
    */
  def disconnect(): Unit =
    close()
}
