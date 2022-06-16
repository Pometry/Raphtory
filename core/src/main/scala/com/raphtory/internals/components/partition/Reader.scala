package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.EndQuery
import com.raphtory.internals.components.querymanager.EstablishExecutor
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[raphtory] class Reader(
    partitionID: Int,
    storage: GraphPartition,
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {

  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))
  private val executorMap      = mutable.Map[String, QueryExecutor]()
  private val watermarkPublish = topics.watermark.endPoint

  private var scheduledWatermark: Option[() => Future[Unit]] = None

  override def run(): Unit = {
    logger.debug(s"Partition $partitionID: Starting Reader Consumer.")
    scheduleWatermarker()
  }

  override def stop(): Unit = {
    scheduledWatermark.foreach(cancelable => cancelable())
    watermarkPublish.close()
    executorMap.synchronized {
      executorMap.foreach(_._2.stop())
    }
  }

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case EstablishExecutor(jobID, sink) =>
        val queryExecutor =
          new QueryExecutor(partitionID, sink, storage, jobID, conf, topics, scheduler)
        scheduler.execute(queryExecutor)
        telemetry.queryExecutorCollector.labels(partitionID.toString, deploymentID).inc()
        executorMap += ((jobID, queryExecutor))

      case req: EndQuery                  =>
        logger.debug(s"Reader on partition $partitionID received $req")
        executorMap.synchronized {
          try {
            executorMap(req.jobID).stop()
            executorMap.remove(req.jobID)
            telemetry.queryExecutorCollector.labels(partitionID.toString, deploymentID).dec()
          }
          catch {
            case e: Exception =>
              e.printStackTrace()
          }
        }
    }

  private def checkWatermark(): Unit = {
    storage.watermarker.updateWatermark()
    val latestWatermark = storage.watermarker.getLatestWatermark
    watermarkPublish sendAsync latestWatermark
    telemetry.lastWatermarkProcessedCollector
      .labels(partitionID.toString, deploymentID)
      .set(latestWatermark.oldestTime.toDouble)
    scheduleWatermarker()
  }

  private def scheduleWatermarker(): Unit = {
    logger.trace("Scheduled watermarker to recheck time in 1 second.")
    scheduledWatermark = Option(
            scheduler
              .scheduleOnce(1.seconds, checkWatermark())
    )

  }

}

object Reader {

  def apply[IO[_]: Async: Spawn](
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
            List(topics.queryPrep),
            new Reader(partitionID, storage, scheduler, conf, topics)
    )
}
