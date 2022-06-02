package com.raphtory.components.partition

import com.raphtory.communication.TopicRepository
import com.raphtory.components.Component
import com.raphtory.components.querymanager.EndQuery
import com.raphtory.components.querymanager.EstablishExecutor
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.WatermarkTime
import com.raphtory.config.MonixScheduler
import com.raphtory.graph.GraphPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Cancelable
import org.apache.pulsar.client.api.Consumer
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

/** @note DoNotDocument */
class Reader(
    partitionID: Int,
    storage: GraphPartition,
    scheduler: MonixScheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[QueryManagement](conf) {

  private val logger: Logger   = Logger(LoggerFactory.getLogger(this.getClass))
  private val executorMap      = mutable.Map[String, QueryExecutor]()
  private val watermarkPublish = topics.watermark.endPoint

  private val queryPrepListener                      =
    topics.registerListener(s"$deploymentID-reader-$partitionID", handleMessage, topics.queryPrep)
  private var scheduledWatermark: Option[Cancelable] = None

  override def run(): Unit = {
    logger.debug(s"Partition $partitionID: Starting Reader Consumer.")
    queryPrepListener.start()
    scheduleWatermarker()
  }

  override def stop(): Unit = {
    queryPrepListener.close()
    scheduledWatermark.foreach(_.cancel())
    watermarkPublish.close()
    executorMap.synchronized {
      executorMap.foreach(_._2.stop())
    }
  }

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case req: EstablishExecutor =>
        val jobID         = req.jobID
        val queryExecutor = new QueryExecutor(partitionID, storage, jobID, conf, topics, scheduler)
        scheduler.execute(queryExecutor)
        telemetry.queryExecutorCollector.labels(partitionID.toString, deploymentID).inc()
        executorMap += ((jobID, queryExecutor))

      case req: EndQuery          =>
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
      .set(latestWatermark.oldestTime)
    scheduleWatermarker()
  }

  private def scheduleWatermarker(): Unit = {
    logger.trace("Scheduled watermarker to recheck time in 1 second.")
    scheduledWatermark = scheduler
      .scheduleOnce(1.seconds, checkWatermark())

  }

}
