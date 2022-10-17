package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
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

class RaphtoryContext(serviceResource: Resource[IO, RaphtoryService[IO]], config: Config, local: Boolean = true) {
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  // With this API user is trying to connect to an existing graph with following expectations:
  // - if the graph is not available with the provided graph id, throw an exception otherwise fetch the graph to work with
  def runWithGraph[T](graphID: String, destory: Boolean = false)(f: DeployedTemporalGraph => T): T = {
    def newIOGraph(graphID: String = createName): Resource[IO, DeployedTemporalGraph] =
      for {
        service       <- serviceResource
        ifGraphExists <- Resource.eval(service.getGraph(protocol.GetGraph(graphID)))
        _              = if (!ifGraphExists.success) throw NoGraphFound(graphID)
        _             <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
        topicRepo     <- LocalTopicRepository[IO](config, None)
        querySender   <-
          Resource.make(IO.delay(new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName))) {
            qs =>
              IO.blocking {
                if (destory) qs.destroyGraph(true) else qs.disconnect()
              }
          }
      } yield new DeployedTemporalGraph(Query(graphID = graphID), querySender, config, local, IO.unit)

    newIOGraph(graphID)
      .use { graph =>
        IO.blocking(f(graph))
      }
      .unsafeRunSync()
  }

  // With this API user is trying to create a fresh graph with following expectations:
  // - if no graph is available with the provided graph id, create a new graph, otherwise throw an exception
  def runWithNewGraph[T](graphID: String = createName, destory: Boolean = false)(f: DeployedTemporalGraph => T): T = {
    def newIOGraph(graphID: String = createName): Resource[IO, DeployedTemporalGraph] =
      for {
        service       <- serviceResource
        ifGraphExists <- Resource.eval(service.getGraph(protocol.GetGraph(graphID)))
        _              = if (ifGraphExists.success) throw GraphAlreadyDeployed(graphID)
        _             <- Prometheus[IO](config.getInt("raphtory.prometheus.metrics.port"))
        topicRepo     <- LocalTopicRepository[IO](config, None)
        querySender   <-
          Resource.make(IO.delay(new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName))) {
            qs =>
              IO.blocking {
                if (destory) qs.destroyGraph(true) else qs.disconnect()
              }
          }
        _             <- Resource.eval(IO(querySender.establishGraph()))
      } yield new DeployedTemporalGraph(Query(graphID = graphID), querySender, config, local, IO.unit)

    newIOGraph(graphID)
      .use { graph =>
        IO.blocking(f(graph))
      }
      .unsafeRunSync()
  }

  def destroyGraph(graphID: String): Unit = runWithGraph(graphID, destory = true) { _ => }
}

object RaphtoryContext {
  private lazy val deployInterface = defaultConf.getString("raphtory.deploy.address")
  private lazy val deployPort      = defaultConf.getInt("raphtory.deploy.port")

  class RaphtoryContextBuilder {

    final def local(): (Resource[IO, RaphtoryService[IO]], RaphtoryContext) = {
      val service = RaphtoryServiceBuilder.standalone[IO](defaultConf)
      val ctx     = new RaphtoryContext(service, defaultConf, true)
      (service, ctx)
    }

    final def remote(
        interface: String = deployInterface,
        port: Int = deployPort
    ): (Resource[IO, RaphtoryService[IO]], RaphtoryContext) = {
      val config =
        ConfigBuilder()
          .addConfig("raphtory.deploy.address", interface)
          .addConfig("raphtory.deploy.port", port)
          .build()
          .getConfig

      val service = RaphtoryServiceBuilder.client[IO](config)
      val ctx     = new RaphtoryContext(service, config, false)
      (service, ctx)
    }
  }

  object RaphtoryContextBuilder {
    def apply(): RaphtoryContextBuilder = new RaphtoryContextBuilder()
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
