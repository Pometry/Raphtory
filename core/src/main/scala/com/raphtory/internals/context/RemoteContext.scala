package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.Raphtory.confBuilder
import com.raphtory.Raphtory.connect
import com.raphtory.Raphtory.makeIdManager
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.Py4JServer
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.RpcClient
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.context.LocalContext.createName

import scala.collection.mutable

class RemoteContext(address: String, port: Int, deploymentID: String) extends RaphtoryContext {

  private def connectManaged(
      graphID: String,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, (QuerySender, Config)] = {
    val userParameters = List(
            Some("raphtory.graph.id" -> graphID),
            if (address.isEmpty) None else Some("raphtory.deploy.address", address),
            if (port == 0) None else Some("raphtory.deploy.port", port),
            if (deploymentID.isEmpty) None else Some("raphtory.deploy.id", deploymentID)
    ).collect {
      case Some(tuple) => tuple
    }.toMap

    val config         = confBuilder(userParameters ++ customConfig)
    val prometheusPort = config.getInt("raphtory.prometheus.metrics.port")
    for {
      _               <- Py4JServer.fromEntryPoint[IO](this, config)
      _               <- Prometheus[IO](prometheusPort)
      topicRepo       <- LocalTopicRepository[IO](config)
      sourceIdManager <- makeIdManager[IO](config, localDeployment = false, graphID, forPartitions = false)
      _               <- RpcClient[IO](topicRepo, config)
      querySender      = new QuerySender(new Scheduler(), topicRepo, config, sourceIdManager, createName)
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
