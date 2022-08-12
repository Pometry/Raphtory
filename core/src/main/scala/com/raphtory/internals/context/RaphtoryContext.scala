package com.raphtory.internals.context

import cats.effect.IO
import com.oblac.nomen.Nomen
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.management._
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

abstract class RaphtoryContext {

  protected case class Metadata(
      graphID: String,
      conf: Config
  )

  protected case class Deployment(metadata: Metadata, deployed: DeployedTemporalGraph)

  private[raphtory] class GraphAlreadyDeployedException(message: String) extends Exception(message)

  protected val logger: Logger                            = Logger(LoggerFactory.getLogger(this.getClass))
  protected var services: mutable.Map[String, Deployment] = mutable.Map.empty[String, Deployment]

  def newGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph

  def getGraph(graphID: String): Option[DeployedTemporalGraph] =
    services.synchronized(services.get(graphID) match {
      case Some(deployment) => Some(deployment.deployed)
      case None             => None
    })

  protected def createName: String =
    Nomen.est().adjective().color().animal().get()

  def close(): Unit

  private[raphtory] def confBuilder(
      customConfig: Map[String, Any] = Map()
  ): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig()
  }

}
