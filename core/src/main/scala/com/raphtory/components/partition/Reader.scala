package com.raphtory.components.partition

import com.raphtory.components.Component
import com.raphtory.components.querymanager.EndQuery
import com.raphtory.components.querymanager.EstablishExecutor
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.WatermarkTime
import com.raphtory.config.Gateway
import com.raphtory.config.PulsarController
import com.raphtory.graph.GraphPartition
import com.typesafe.config.Config
import monix.execution.Cancelable
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Consumer

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** @DoNotDocument */
class Reader(
    partitionID: Int,
    storage: GraphPartition,
    scheduler: Scheduler,
    conf: Config,
    gateway: Gateway
) extends Component[QueryManagement](conf, gateway) {
  private val executorMap                               = mutable.Map[String, QueryExecutor]()
  private val watermarkPublish                          = gateway.toWatermark
  var cancelableConsumer: Option[Consumer[Array[Byte]]] = None
  var scheduledWatermark: Option[Cancelable]            = None
  private var lastWatermark                             = WatermarkTime(partitionID, Long.MaxValue, Long.MinValue, false)

  private val watermarking = new Runnable {
    override def run(): Unit = createWatermark()
  }

  override def setup(): Unit = {
    logger.debug(s"Partition $partitionID: Starting Reader Consumer.")

    scheduleWaterMarker()
  }

  override def stopHandler(): Unit = {
    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }
    scheduledWatermark.foreach(_.cancel())
    watermarkPublish.close()
    executorMap.foreach(_._2.stop())
  }

  override def handleMessage(msg: QueryManagement): Unit =
    msg match {
      case req: EstablishExecutor =>
        val jobID         = req.jobID
        val queryExecutor = new QueryExecutor(partitionID, storage, jobID, conf, pulsarController)
        scheduler.execute(queryExecutor)
        executorMap += ((jobID, queryExecutor))

      case req: EndQuery          =>
        executorMap(req.jobID).stop()
        executorMap.remove(req.jobID)
    }

  def createWatermark(): Unit = {
    if (!storage.currentyBatchIngesting()) {
      val newestTime              = storage.newestTime
      val oldestTime              = storage.oldestTime
      val blockingEdgeAdditions   = storage.blockingEdgeAdditions.nonEmpty
      val blockingEdgeDeletions   = storage.blockingEdgeDeletions.nonEmpty
      val blockingVertexDeletions = storage.blockingVertexDeletions.nonEmpty

      //this is quite ugly but will be fully written with the new semaphore implementation
      val BEAtime = storage.blockingEdgeAdditions.reduceOption[(Long, (Long, Long))]({
        case (update1, update2) =>
          if (update1._1 < update2._1) update1 else update2
      }) match {
        case Some(value) => value._1 //get out the timestamp
        case None        => newestTime
      }

      val BEDtime = storage.blockingEdgeDeletions.reduceOption[(Long, (Long, Long))]({
        case (update1, update2) =>
          if (update1._1 < update2._1) update1 else update2
      }) match {
        case Some(value) => value._1 //get out the timestamp
        case None        => newestTime
      }

      val BVDtime = storage.blockingVertexDeletions.reduceOption[((Long, Long), AtomicInteger)]({
        case (update1, update2) =>
          if (update1._1._1 < update2._1._1) update1 else update2
      }) match {
        case Some(value) => value._1._1 //get out the timestamp
        case None        => newestTime
      }

      val finalTime = Array(newestTime, BEAtime, BEDtime, BVDtime).min

      val noBlockingOperations =
        !blockingEdgeAdditions && !blockingEdgeDeletions && !blockingVertexDeletions
      val watermark            = WatermarkTime(partitionID, oldestTime, finalTime, noBlockingOperations)
      if (watermark != lastWatermark) {
        logger.debug(
                s"Partition $partitionID: Creating watermark with " +
                  s"earliest time '$oldestTime' and latest time '$finalTime'."
        )
        watermarkPublish.sendAsync(
                serialise(watermark)
        )
      }
      lastWatermark = watermark
    }
    scheduleWaterMarker()
  }

  private def scheduleWaterMarker(): Unit = {
    logger.trace("Scheduled watermarker to recheck time in 1 second.")
    scheduledWatermark = Some(
            scheduler
              .scheduleOnce(1, TimeUnit.SECONDS, watermarking)
    )
  }

}
