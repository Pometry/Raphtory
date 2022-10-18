package com.raphtory.internals.context

import cats.effect.IO
import cats.effect.Resource
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.internals.communication.repositories.ArrowFlightRepository
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.communication.repositories.LocalTopicRepository
import com.raphtory.internals.components.ingestion.IngestionManager
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.management.Prometheus
import com.raphtory.internals.management.QuerySender
import com.raphtory.internals.management.Scheduler
import com.raphtory.protocol
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.Raphtory.makeLocalIdManager
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.ZookeeperConnector
import com.typesafe.config.Config
import cats.effect.unsafe.implicits.global
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.components.RaphtoryServiceBuilder
import com.raphtory.internals.components.partition.PartitionOrchestrator
import com.raphtory.internals.management.arrow.LocalHostAddressProvider
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import org.apache.arrow.memory.RootAllocator

import scala.collection.mutable

private[raphtory] object LocalContext extends RaphtoryContext {

  def newGraph(graphID: String = createName, customConfig: Map[String, Any] = Map()): DeployedTemporalGraph =
    services.synchronized {
      val config                  = confBuilder(customConfig)
      val graph                   = deployService(graphID, config)
      services.get(graphID) match {
        case Some(_) =>
          throw new GraphAlreadyDeployedException(s"The graph '$graphID' already exists")
        case None    =>
          logger.info(s"Creating Service for '$graphID'")
      }
      val (querySender, shutdown) = graph.allocated.unsafeRunSync()
      val deployed                = new DeployedTemporalGraph(Query(graphID = graphID), querySender, config, local = true, shutdown)
      val deployment              = Deployment(Metadata(graphID, config), deployed)
      services += ((graphID, deployment))
      deployed
    }

  def newIOGraph(
      graphID: String = createName,
      customConfig: Map[String, Any] = Map()
  ): Resource[IO, DeployedTemporalGraph] = {
    val config = confBuilder(customConfig)
    deployService(graphID, config).map { qs: QuerySender =>
      new DeployedTemporalGraph(Query(graphID = graphID), qs, config, local = true, shutdown = IO.unit)
    }
  }

  private def deployService(graphID: String, config: Config): Resource[IO, QuerySender] = {
    val scheduler = new Scheduler()
    for {
      arrowServer   <- ArrowFlightServer[IO]()
      addressHandler = new LocalHostAddressProvider(config, arrowServer)
      topicRepo     <- ArrowFlightRepository[IO](config, addressHandler)
      service       <- RaphtoryServiceBuilder.standalone[IO](config)
      clientId      <- Resource.eval(IO(createName))
      _             <- Resource.eval(service.establishGraph(protocol.ClientGraphId(clientId, graphID)))
    } yield new QuerySender(graphID, service, scheduler, topicRepo, config, clientId)
  }

  override def close(): Unit = {
    services.values.foreach(deployment => deployment.deployed.close())
    services = mutable.Map.empty[String, Deployment]
  }

}
