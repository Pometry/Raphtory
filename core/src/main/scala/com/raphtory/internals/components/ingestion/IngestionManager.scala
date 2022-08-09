package com.raphtory.internals.components.ingestion

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.IngestData
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

class IngestionManager(
    conf: Config,
    topics: TopicRepository
) extends Component[IngestData](conf) {

  case class Graph(ingestionCancel: IO[Unit]) {
    def stop(): Unit = ingestionCancel.unsafeRunSync()
  }

  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))
  private val graphDeployments = mutable.Map[String, Graph]()

  override def handleMessage(msg: IngestData): Unit =
    msg match {
      case IngestData(loader, graphID, sources) =>
        logger.debug(s"Received query to spawn graph: $msg")
        sources foreach { source =>
          val ingestionResource    = IngestionExecutor[IO](graphID, source, conf, topics)
          val (_, ingestionCancel) = ingestionResource.allocated.unsafeRunSync()
          graphDeployments.put(graphID, Graph(ingestionCancel))
        }
    }

  override private[raphtory] def run(): Unit = logger.debug(s"Starting IngestionManager")

  override private[raphtory] def stop(): Unit =
    graphDeployments.foreach { case (graphID, _) => graphDeployments.remove(graphID).foreach(_.stop()) }
}

object IngestionManager {

  def apply[IO[_]: Async: Spawn](
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, IngestionManager] =
    Component.makeAndStart(
            topics,
            s"ingestion-manager",
            List(topics.ingestSetup),
            new IngestionManager(conf, topics)
    )
}
