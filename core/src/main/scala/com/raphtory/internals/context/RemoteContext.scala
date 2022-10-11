package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.{ConfigBuilder, Prometheus, QuerySender, Scheduler}
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.createName
import com.raphtory.internals.components.RaphtoryServiceBuilder

import scala.collection.mutable

class RemoteContext(address: String, port: Int) extends RaphtoryContext {

  private def connectManaged(
      graphID: String,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, (QuerySender, Config)] = {
    val userParameters = List(
            if (address.isEmpty) None else Some("raphtory.deploy.address", address),
            if (port == 0) None else Some("raphtory.deploy.port", port)
    ).collect {
      case Some(tuple) => tuple
    }.toMap

    val config         = ConfigBuilder.build(userParameters ++ customConfig).getConfig
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _          <- Prometheus[IO](prometheusPort)
      service    <- RaphtoryServiceBuilder.client[IO](config)
      topicRepo  <- LocalTopicRepository[IO](config, None)
      querySender = new QuerySender(graphID, service, new Scheduler(), topicRepo, config, createName)
    } yield (querySender, config)
  }

  override def newGraph(graphID: String, customConfig: Map[String, Any]): DeployedTemporalGraph =
    services.synchronized(services.get(graphID) match {
      case Some(_) => throw new GraphAlreadyDeployedException(s"The graph '$graphID' already exists")
      case None    =>
        val managed: IO[((QuerySender, Config), IO[Unit])] = connectManaged(graphID, customConfig).allocated
        val ((querySender, config), shutdown)              = managed.unsafeRunSync()
        val graph                                          = Metadata(graphID, config)
        val deployed                                       = new DeployedTemporalGraph(Query(graphID = graphID), querySender, config, local = false, shutdown)
        val deployment                                     = Deployment(graph, deployed)
        services += ((graphID, deployment))
        querySender.establishGraph()
        deployed
    })

  override def close(): Unit =
    services.synchronized {
      services.values.foreach(_.deployed.close())
      services = mutable.Map.empty[String, Deployment]
    }

}
