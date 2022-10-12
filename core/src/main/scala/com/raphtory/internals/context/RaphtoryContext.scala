package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.createName
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.protocol.RaphtoryService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.internals.components.querymanager.Query

class RaphtoryContext(serviceResource: Resource[IO, RaphtoryService[IO]], config: Config, local: Boolean = true) {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def newGraph(graphID: String = createName): DeployedTemporalGraph = {
    val qs = for {
      _          <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      service    <- serviceResource
      topicRepo  <- LocalTopicRepository[IO](config, None)
      querySender = new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName)
      _          <- Resource.eval(IO(querySender.establishGraph()))
    } yield querySender

    val (querySender, shutdown) = qs.allocated.unsafeRunSync()
    new DeployedTemporalGraph(Query(graphID = graphID), querySender, config, local, shutdown)
  }

  def getGraph(graphID: String): DeployedTemporalGraph = {
    val qs = for {
      _          <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      service    <- serviceResource
      topicRepo  <- LocalTopicRepository[IO](config, None)
      querySender = new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName)
    } yield querySender

    val (querySender, shutdown) = qs.allocated.unsafeRunSync()
    new DeployedTemporalGraph(Query(graphID = graphID), querySender, config, local, shutdown)
  }
}
