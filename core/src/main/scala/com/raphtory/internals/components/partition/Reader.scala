package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.Topic
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.BlockIngestion
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.UnblockIngestion
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[raphtory] class Reader(
    graphID: String,
    partitionID: Int,
    storage: GraphPartition,
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {

  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))
  private val watermarkPublish = topics.watermark(graphID).endPoint

  private var scheduledWatermark: Option[() => Future[Unit]] = None

  override def run(): Unit = {
    logger.debug(s"Partition $partitionID: Starting Reader Consumer.")
    scheduleWatermarker()
  }

  override def stop(): Unit = {
    scheduledWatermark.foreach(cancelable => cancelable())
    watermarkPublish.close()
  }

  private def checkWatermark(): Unit = {
    storage.watermarker.updateWatermark()
    val latestWatermark = storage.watermarker.getLatestWatermark
    watermarkPublish sendAsync latestWatermark
    telemetry.lastWatermarkProcessedCollector
      .labels(partitionID.toString, graphID)
      .set(latestWatermark.oldestTime.toDouble)
    scheduleWatermarker()
  }

  private def scheduleWatermarker(): Unit =
    scheduledWatermark = Option(
            scheduler
              .scheduleOnce(100.milliseconds, checkWatermark())
    )

  override def handleMessage(msg: QueryManagement): Unit = {}
}

object Reader {

  def apply[IO[_]: Async](
      graphID: String,
      partitionID: Int,
      storage: GraphPartition,
      scheduler: Scheduler,
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, Reader] =
    Component.makeAndStartPart(
            partitionID,
            topics,
            s"reader-$partitionID",
            List[Topic[QueryManagement]](),
            new Reader(graphID, partitionID, storage, scheduler, conf, topics)
    )
}
