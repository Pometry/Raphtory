package com.raphtory.core.components.partition

import com.raphtory.core.components.querymanager.EstablishExecutor
import com.raphtory.core.components.Component
import com.raphtory.core.components.querymanager.EstablishExecutor
import com.raphtory.core.components.querymanager.WatermarkTime
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.GraphPartition
import com.typesafe.config.Config
import monix.eval.Task
import monix.execution.Scheduler
import org.apache.pulsar.client.admin.PulsarAdminException
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

/** @DoNotDocument */
class Reader(
    partitionID: Int,
    storage: GraphPartition,
    scheduler: Scheduler,
    conf: Config,
    pulsarController: PulsarController
) extends Component[EstablishExecutor](conf: Config, pulsarController: PulsarController) {

  private val executorMap                               = mutable.Map[String, QueryExecutor]()
  private val watermarkPublish                          = pulsarController.watermarkPublisher()
  var cancelableConsumer: Option[Consumer[Array[Byte]]] = None

  override def run(): Unit = {
    logger.debug(s"Partition $partitionID: Starting Reader Consumer.")

    scheduleWaterMarker()

    cancelableConsumer = Some(
            pulsarController.startReaderConsumer(partitionID, messageListener())
    )
  }

  override def stop(): Unit = {
    cancelableConsumer match {
      case Some(value) =>
        value.close()
      case None        =>
    }
    watermarkPublish.close()
    executorMap.foreach(_._2.stop())
  }

  override def handleMessage(msg: EstablishExecutor): Unit = {
    val jobID         = msg.jobID
    val queryExecutor = new QueryExecutor(partitionID, storage, jobID, conf, pulsarController)

    scheduler.execute(queryExecutor)

    executorMap += ((jobID, queryExecutor))
  }

  def createWatermark(): Unit = {
    if (!storage.currentyBatchIngesting()) {
      val newestTime              = storage.newestTime
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

      logger.trace(s"Partition $partitionID: Creating watermark at '$finalTime'.")

      if (!blockingEdgeAdditions && !blockingEdgeDeletions && !blockingVertexDeletions)
        watermarkPublish.sendAsync(serialise(WatermarkTime(partitionID, finalTime, true)))
      else
        watermarkPublish.sendAsync(serialise(WatermarkTime(partitionID, finalTime, false)))

    }
    scheduleWaterMarker()
  }

  private def scheduleWaterMarker(): Unit = {
    val watermarking = new Runnable {
      override def run(): Unit = createWatermark()
    }

    scheduler
      .scheduleOnce(1, TimeUnit.SECONDS, watermarking)

  }

}
