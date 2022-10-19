package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory._
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
import com.raphtory.internals.context.GraphException._

class RaphtoryContext(serviceAsResource: Resource[IO, RaphtoryService[IO]], config: Config) {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  // With this API user is trying to connect to an existing graph with following expectations:
  // - if the graph is not available with the provided graph id, throw an exception otherwise fetch the graph to work with
  def runWithGraph[T](graphID: String, destroy: Boolean = false)(f: DeployedTemporalGraph => T): T = {
    def newIOGraph(graphID: String = createName): Resource[IO, DeployedTemporalGraph] =
      for {
        service       <- serviceAsResource
        ifGraphExists <- Resource.eval(service.getGraph(protocol.GetGraph(graphID)))
        _              = if (!ifGraphExists.success) throw NoGraphFound(graphID)
        _             <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
        topicRepo     <- LocalTopicRepository[IO](config, None)
        querySender   <-
          Resource.make(IO.delay(new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName))) {
            qs =>
              IO.blocking {
                if (destroy) qs.destroyGraph(true) else qs.disconnect()
              }
          }
      } yield new DeployedTemporalGraph(Query(graphID = graphID), querySender, config)

    newIOGraph(graphID)
      .use { graph =>
        IO.blocking(f(graph))
      }
      .unsafeRunSync()
  }

  // With this API user is trying to create a fresh graph with following expectations:
  // - if no graph is available with the provided graph id, create a new graph, otherwise throw an exception
  def runWithNewGraph[T](graphID: String = createName, destroy: Boolean = false)(f: DeployedTemporalGraph => T): T = {
    def newIOGraph(graphID: String = createName): Resource[IO, DeployedTemporalGraph] =
      for {
        service       <- serviceAsResource
        ifGraphExists <- Resource.eval(service.getGraph(protocol.GetGraph(graphID)))
        _              = if (ifGraphExists.success) throw GraphAlreadyDeployed(graphID)
        _             <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
        topicRepo     <- LocalTopicRepository[IO](config, None)
        querySender   <-
          Resource.make(IO.delay(new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName))) {
            qs =>
              IO.blocking {
                if (destroy) qs.destroyGraph(true) else qs.disconnect()
              }
          }
        _             <- Resource.eval(IO(querySender.establishGraph()))
      } yield new DeployedTemporalGraph(Query(graphID = graphID), querySender, config)

    newIOGraph(graphID)
      .use { graph =>
        IO.blocking(f(graph))
      }
      .unsafeRunSync()
  }

  def destroyGraph(graphID: String): Unit = runWithGraph(graphID, destroy = true) { _ => }
}

object GraphException {
  final case class GraphAlreadyDeployed private (message: String) extends Exception(message)

  object GraphAlreadyDeployed {

    def apply(graphID: String): GraphAlreadyDeployed = {
      val msg =
        s"A graph already exists with graph id = $graphID. Should you choose to work with an existing graph, use `runWithGraph` API instead otherwise try again with a new graph id."
      new GraphAlreadyDeployed(msg)
    }
  }

  final case class NoGraphFound private (message: String) extends Exception(message)

  object NoGraphFound {

    def apply(graphID: String): NoGraphFound = {
      val msg =
        s"No graph found with graph id = $graphID. Are you sure if the provided graph id is correct? Should you choose to create a new graph, use `runWithNewGraph` API instead."
      new NoGraphFound(msg)
    }
  }
}
