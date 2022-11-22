package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.PyDeployedTemporalGraph
import com.raphtory._
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.protocol.RaphtoryService
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.context.GraphException._
import com.raphtory.internals.management.GraphConfig.ConfigBuilder
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema

abstract class Context(serviceAsResource: Resource[IO, RaphtoryService[IO]], config: Config) {

  protected def newIOGraphParams(
      failOnNotFound: Boolean,
      graphID: String = createName,
      destroy: Boolean
  ): Resource[IO, (Query, QuerySender, Config)] =
    for {
      service       <- serviceAsResource
      ifGraphExists <- Resource.eval(service.getGraph(protocol.GraphId(graphID)))
      _              = if (!ifGraphExists.success && failOnNotFound) throw NoGraphFound(graphID)
                       else if (!failOnNotFound && ifGraphExists.success) throw GraphAlreadyDeployed(graphID)
      _             <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
      topicRepo     <- LocalTopicRepository[IO](config, None)
      querySender   <- Resource.make(IO.delay(new QuerySender(graphID, service, topicRepo, config, createName))) { qs =>
                         IO.blocking {
                           if (destroy) qs.destroyGraph(true) else qs.disconnect()
                         }
                       }
      _             <- { if (!failOnNotFound) Resource.eval(IO(querySender.establishGraph())) else Resource.eval(IO.unit) }
    } yield (Query(graphID = graphID), querySender, config)
}

class PyRaphtoryContext(serviceAsResource: Resource[IO, RaphtoryService[IO]], config: Config, shutdown: IO[Unit])
        extends Context(serviceAsResource, config)
        with AutoCloseable {

  def newGraph(graphID: String = createName, persist: Boolean = false): PyDeployedTemporalGraph = {
    val ((q, qs, c), shutdown) =
      newIOGraphParams(failOnNotFound = false, graphID, destroy = !persist).allocated.unsafeRunSync()
    new PyDeployedTemporalGraph(q, qs, c, shutdown)
  }

  def getGraph(graphID: String): PyDeployedTemporalGraph = {
    val ((q, qs, c), shutdown) =
      newIOGraphParams(failOnNotFound = true, graphID, destroy = false).allocated.unsafeRunSync()
    new PyDeployedTemporalGraph(q, qs, c, shutdown)
  }

  def close(): Unit = shutdown.unsafeRunSync()
}

object PyRaphtoryContext {

  private lazy val (standalone, shutdown): (RaphtoryService[IO], IO[Unit]) =
    RaphtoryServiceBuilder.standalone[IO](defaultConf).allocated.unsafeRunSync()

  def local(): PyRaphtoryContext = new PyRaphtoryContext(Resource.pure(standalone), defaultConf, shutdown)

  def remote(host: String, port: Int): PyRaphtoryContext = {
    val config =
      ConfigBuilder()
        .addConfig("raphtory.deploy.address", host)
        .addConfig("raphtory.deploy.port", port)
        .build()
        .getConfig

    val service = RaphtoryServiceBuilder.client[IO](config)
    new PyRaphtoryContext(service, config, IO.unit)
  }
}

class RaphtoryContext(serviceAsResource: Resource[IO, RaphtoryService[IO]], config: Config)
        extends Context(serviceAsResource, config) {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def newIOGraph(
      failOnNotFound: Boolean,
      graphID: String = createName,
      destroy: Boolean
  ): Resource[IO, DeployedTemporalGraph] =
    newIOGraphParams(failOnNotFound, graphID, destroy).map {
      case (query, querySender, conf) =>
        new DeployedTemporalGraph(query, querySender, conf)
    }

  def newArrowIOGraph[V: VertexSchema, E: EdgeSchema](
      failOnNotFound: Boolean,
      graphID: String = createName,
      destroy: Boolean
  ): Resource[IO, DeployedTemporalGraph] =
    newIOGraphParams(failOnNotFound, graphID, destroy).map {
      case (query, querySender, conf) =>
        new DeployedTemporalGraph(query, querySender, conf)
    }

  // With this API user is trying to connect to an existing graph with following expectations:
  // - if the graph is not available with the provided graph id, throw an exception otherwise fetch the graph to work with
  def runWithGraph[T](graphID: String, destroy: Boolean = false)(f: DeployedTemporalGraph => T): T =
    newIOGraph(failOnNotFound = true, graphID, destroy)
      .use { graph =>
        IO.blocking(f(graph))
      }
      .unsafeRunSync()

  // With this API user is trying to create a fresh graph with following expectations:
  // - if no graph is available with the provided graph id, create a new graph, otherwise throw an exception
  def runWithNewGraph[T](graphID: String = createName, persist: Boolean = false)(f: DeployedTemporalGraph => T): T =
    newIOGraph(failOnNotFound = false, graphID, !persist)
      .use { graph =>
        IO.blocking(f(graph))
      }
      .unsafeRunSync()

  def destroyGraph(graphID: String): Unit = runWithGraph(graphID, destroy = true) { _ => }
}

object RaphtoryIOContext {

  def localIO(): Resource[IO, RaphtoryContext] =
    RaphtoryServiceBuilder
      .standalone[IO](defaultConf)
      .evalMap(service => IO(new RaphtoryContext(Resource.pure(service), defaultConf)))

  def remoteIO(host: String = deployInterface, port: Int = deployPort): Resource[IO, RaphtoryContext] = {
    val config =
      ConfigBuilder()
        .addConfig("raphtory.deploy.address", host)
        .addConfig("raphtory.deploy.port", port)
        .build()
        .getConfig

    val service = RaphtoryServiceBuilder.client[IO](config)
    Resource.pure(new RaphtoryContext(service, config))
  }
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
