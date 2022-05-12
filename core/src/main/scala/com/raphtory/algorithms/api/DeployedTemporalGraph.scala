package com.raphtory.algorithms.api

import com.raphtory.client.QuerySender
import com.raphtory.components.querymanager.Query
import com.typesafe.config.Config

/** Root class for local deployments of the analysis API
  *
  * A DeployedTemporalGraph is a TemporalGraph
  * with a deployment object attached to it that allows to stop it.
  *
  * @see [[com.raphtory.algorithms.api.TemporalGraph]]
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

  /** Access to the deployment to call functions upon it, e.g. graph.deployment.stop()
    *
    * @return Deployment reference
    * */
  def deployment: Deployment = deploymentRef

  private[raphtory] def getConfig() = conf
}
