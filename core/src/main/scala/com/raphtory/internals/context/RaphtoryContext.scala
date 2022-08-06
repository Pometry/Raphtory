package com.raphtory.internals.context

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import com.oblac.nomen.Nomen
import com.raphtory.Raphtory.makePartitionIdManager
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.management._
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

abstract class RaphtoryContext {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def newGraph(name: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph

  protected def createName: String =
    Nomen.est().adjective().color().animal().get()

  def close()

  private[raphtory] def confBuilder(
      customConfig: Map[String, Any] = Map()
  ): Config = {
    val confHandler = new ConfigHandler()
    customConfig.foreach { case (key, value) => confHandler.addCustomConfig(key, value) }
    confHandler.getConfig()
  }

}
