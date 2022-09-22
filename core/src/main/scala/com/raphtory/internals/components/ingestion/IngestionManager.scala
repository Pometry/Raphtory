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

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  private val executors      = mutable.ArrayBuffer[IO[Unit]]()

  override def handleMessage(msg: IngestData): Unit =
    msg match { //TODO disconnect/destroy source
      case IngestData(_, graphID, sourceID, sources, blocking) =>
        logger.info(s"Ingestion Manager for '$graphID' establishing new data source")
        executors.synchronized {
          sources foreach {
            case (id, source) =>
              val ingestionResource    = IngestionExecutor[IO](graphID, source, blocking, id, conf, topics)
              val (_, ingestionCancel) = ingestionResource.allocated.unsafeRunSync()
              executors += ingestionCancel

          }
        }
    }

  override private[raphtory] def run(): Unit = logger.debug(s"Starting IngestionManager")

  override private[raphtory] def stop(): Unit =
    executors.synchronized {
      executors.foreach(executor => executor.unsafeRunSync())
    }
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
