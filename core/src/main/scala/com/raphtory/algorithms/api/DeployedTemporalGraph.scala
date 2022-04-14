package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.typesafe.config.Config

/**
  * {s}`DeployedTemporalGraph`
  *  : Root class for local deployments of the analysis API
  *
  * A {s}`DeployedTemporalGraph` is a {s}`TemporalGraph`
  * with a deployment object attached to it that allows to stop it.
  *
  * ## Methods
  *
  *  {s}`deployment: Deployment`
  *    : Access to the deployment to allow stopping it: {s}`graph.deployment.stop()`.
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.TemporalGraph)
  * ```
  */
private[raphtory] class DeployedTemporalGraph(
    query: Query,
    private val querySender: QuerySender,
    private val stopCallBack: () => Unit,
    private val conf: Config
) extends TemporalGraph(query, querySender, conf) {

  class Deployment {
    def stop(): Unit = stopCallBack()
  }
  private val deploymentRef = new Deployment()

  def deployment: Deployment = deploymentRef

  private[raphtory] def getConfig() = conf
}
