package com.raphtory.api.analysis.graphview

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.QuerySender
import com.typesafe.config.Config

/** Root class for local deployments of the analysis API
  *
  * A DeployedTemporalGraph is a [[TemporalGraph]]
  * with a [[Deployment]] object attached to it that allows it to stop.
  * To create a `DeployedTemporalGraph` use the `stream` or `load` method
  * of the [[com.raphtory.Raphtory Raphtory]] object.
  *
  * @param deployment The deployment object used to control Raphtory
  *
  * @see [[TemporalGraph]], [[Deployment]], [[com.raphtory.Raphtory Raphtory]]
  */
class DeployedTemporalGraph private[raphtory] (
    override private[api] val query: Query,
    override private[api] val querySender: QuerySender,
    override private[api] val conf: Config,
    val deploymentId: String,
    private val shutdown: IO[Unit]
) extends TemporalGraph(query, querySender, conf)
        with AutoCloseable {

  def config: Config = conf

  override def close(): Unit = shutdown.unsafeRunSync()
}
