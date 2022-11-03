package com.raphtory.internals.components.cluster

import cats.effect.Async
import cats.effect.std.Dispatcher
import cats.effect.unsafe.implicits.global
import com.raphtory.arrowmessaging.ArrowFlightServer
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.communication.connectors.AkkaConnector
import com.raphtory.internals.communication.repositories.DistributedTopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.ServiceRegistry
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.management.ZookeeperConnector
import com.raphtory.internals.management.id.IDManager
import com.typesafe.config.Config
import com.typesafe.config.ConfigValueFactory
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory
import com.raphtory.internals.management.arrow.ZKHostAddressProvider
import com.raphtory.internals.storage.arrow.EdgeSchema
import com.raphtory.internals.storage.arrow.VertexSchema
import com.raphtory.protocol.PartitionService
import org.apache.arrow.memory.RootAllocator

import scala.collection.mutable

abstract class OrchestratorComponent[F[_]: Async](
    dispatcher: Dispatcher[F],
    partitions: Seq[PartitionService[F]],
    conf: Config
) extends Component[ClusterManagement](conf) {
  private case class Deployment(shutdown: F[Unit], clients: mutable.Set[String])
  private val deployments      = mutable.Map[String, Deployment]()
  protected val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  protected def establishService(
      component: String,
      graphID: String,
      clientID: String,
      topics: TopicRepository,
      func: (String, String, TopicRepository, Config) => Unit
  ): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          logger.info(s"$component for graph '$graphID' already exists, adding '$clientID' to registered users")
          deployment.clients += clientID
        case None             =>
          logger.info(s"Deploying new $component for graph '$graphID' - request by '$clientID' ")
          func(graphID, clientID, topics, conf)
      }
    }

  protected def destroyGraph(graphID: String, clientID: String, force: Boolean): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          deployment.clients.remove(clientID)
          if (force || deployment.clients.isEmpty) {
            logger.info(s"Last client '$clientID' disconnected. Destroying Graph '$graphID'")
            deployments.remove(graphID)
            dispatcher.unsafeRunSync(deployment.shutdown)
          }
          else
            logger.info(s"Client '$clientID' disconnected from Graph '$graphID'")
        case None             => logger.warn(s"Graph '$graphID' requested for destruction by '$clientID', but did not exist")
      }
    }

  protected def clientDisconnected(graphID: String, clientID: String): Unit =
    deployments.synchronized {
      deployments.get(graphID) match {
        case Some(deployment) =>
          deployment.clients.remove(clientID)
          logger.info(s"Client '$clientID' disconnected from Graph '$graphID'")
        case None             => logger.warn(s"'$clientID' disconnected from Graph '$graphID', but did not exist")
      }
    }

  override private[raphtory] def stop(): Unit =
    deployments.foreach {
      case (_, deployment) => dispatcher.unsafeRunSync(deployment.shutdown)
    }

  protected def deployQueryService(
      graphID: String,
      clientID: String,
      topics: TopicRepository,
      conf: Config
  ): Unit =
    deployments.synchronized {
      val serviceResource = QueryManager[F](graphID, dispatcher, partitions, conf, topics)
      val (_, shutdown)   = dispatcher.unsafeRunSync(serviceResource.allocated)
      deployments += ((graphID, Deployment(shutdown, clients = mutable.Set(clientID))))
    }
}
