package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.Raphtory.confBuilder
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.api.analysis.graphview.TemporalGraphConnection
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.Py4JServer
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global

import scala.collection.mutable

class RemoteContext(deploymentID: String) extends RaphtoryContext {

  private val remoteServices: mutable.Map[String, (DeployedTemporalGraph, QuerySender)] =
    mutable.Map.empty[String, (DeployedTemporalGraph, QuerySender)]

  private def connectManaged(
      graphID: String,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, (TopicRepository, Config)] = {
    val config         = confBuilder(Map("raphtory.graph.id" -> graphID, "raphtory.deploy.id" -> deploymentID) ++ customConfig)
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _         <- Py4JServer.fromEntryPoint[IO](this, config)
      _         <- Prometheus[IO](prometheusPort)
      topicRepo <- DistributedTopicRepository[IO](AkkaConnector.ClientMode, config)
    } yield (topicRepo, config)
  }

  override def newGraph(graphID: String, customConfig: Map[String, Any]): DeployedTemporalGraph = {
    val managed: IO[((TopicRepository, Config), IO[Unit])] = connectManaged(graphID, customConfig).allocated
    val ((topicRepo, config), shutdown)                    = managed.unsafeRunSync()
    val querySender                                        = new QuerySender(new Scheduler(), topicRepo, config)
    val graph                                              =
      new DeployedTemporalGraph(Query(), querySender, config, shutdown)
    remoteServices += ((graphID, (graph, querySender)))
    querySender.establishGraph()
    graph
  }

  def destroyRemoteGraph(graphID: String): Unit =
    remoteServices.remove(graphID) match {
      case Some(graph) =>
        graph._2.destroyGraph()
        graph._1.close()
      case None        => logger.warn(s"Trying to destroy remote graph $graphID, but it does not exist")
    }

  override def close(): Unit =
    remoteServices.keys.foreach(graphID =>
      remoteServices.remove(graphID) match {
        case Some(graph) => graph._1.close()
        case None        =>
      }
    )

}
