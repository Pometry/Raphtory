package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.graphbuilder.BuilderExecutor
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

class IngestionManager(
    deploymentID: String,
    conf: Config,
    topics: TopicRepository
) extends Component[EstablishGraph](conf) {

  case class Graph(ingestionCancel: IO[Unit]) {
    def stop(): Unit = ingestionCancel.unsafeRunSync()
  }

  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))
  private val graphDeployments = mutable.Map[String, Graph]()

  override def handleMessage(msg: EstablishGraph): Unit =
    msg match {
      case EstablishGraph(graphID, source) =>
        logger.debug(s"Received query to spawn graph: $msg")
        val ingestionResource    = IngestionExecutor[IO](deploymentID, source, conf, topics)
        val (_, ingestionCancel) = ingestionResource.allocated.unsafeRunSync()
        graphDeployments.put(graphID, Graph(ingestionCancel))
    }

  override private[raphtory] def run(): Unit = logger.debug(s"Starting IngestionManager")

  override private[raphtory] def stop(): Unit =
    graphDeployments.foreach { case (graphID, _) => graphDeployments.remove(graphID).foreach(_.stop()) }
}

object IngestionManager {

  def apply[IO[_]: Async: Spawn](
      deploymentID: String,
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, IngestionManager] =
    Component.makeAndStart(
            topics,
            s"ingestion-manager",
            List(topics.ingestSetup),
            new IngestionManager(deploymentID, conf, topics)
    )
}
