package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.Raphtory.confBuilder
import com.raphtory.Raphtory.connect
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
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.context.LocalContext.createName
import com.raphtory.internals.management.ZookeeperConnector
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.context.LocalContext.createName
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

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

    val config         = confBuilder(userParameters ++ customConfig)
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
