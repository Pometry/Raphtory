package com.raphtory.api.graphview

import com.typesafe.config.Config

/** Public interface for the Raphtory deployment
  *
  * @see [[DeployedTemporalGraph]]
  */
trait Deployment {

  /** The configuration used by the deployment
    */
  val conf: Config

  /** Stop all components and free resources */
  def stop(): Unit
}
