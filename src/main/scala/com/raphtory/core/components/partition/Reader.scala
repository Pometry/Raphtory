package com.raphtory.core.components.partition

import com.raphtory.core.components.Component
import com.raphtory.core.components.querymanager.EstablishExecutor
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.WatermarkTime
import com.raphtory.core.components.querymanager.EstablishExecutor
import com.raphtory.core.config.AsyncConsumer
import com.raphtory.core.config.PulsarController
import com.raphtory.core.graph.GraphPartition
import com.typesafe.config.Config
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Consumer
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.Schema

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class Reader(
    partitionID: Int,
    storage: GraphPartition,
    scheduler: Scheduler,
    conf: Config,
    pulsarController: PulsarController
) extends Component[QueryManagement, EstablishExecutor](
                conf: Config,
                pulsarController: PulsarController,
                scheduler
        ) {

  private val executorMap      = mutable.Map[String, QueryExecutor]()
  private val watermarkPublish = watermarkPublisher()
  override val consumer        = Some(startReaderConsumer(partitionID))

  class Watermarker extends Runnable {

    def run() {
      createWatermark()
    }
  }

  override def run(): Unit = {
    logger.debug(s"Partition $partitionID: Starting Reader Consumer.")

    scheduler.scheduleAtFixedRate(1, 1, TimeUnit.SECONDS, new Watermarker())
    // same scheduler as watermark!
    scheduler.execute(AsyncConsumer(this))
  }

  override def stop(): Unit = {
    consumer match {
      case Some(value) =>
        value.close()
    }
    watermarkPublish.close()
    executorMap.foreach(_._2.stop())
  }

  override def handleMessage(msg: EstablishExecutor): Boolean = {
    val jobID         = msg.jobID
    val queryExecutor =
      new QueryExecutor(partitionID, storage, jobID, conf, pulsarController, scheduler)

    scheduler.execute(queryExecutor)

    executorMap += ((jobID, queryExecutor))
    true
  }

  def createWatermark(): Unit = {
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
      sendMessage(watermarkPublish, WatermarkTime(partitionID, finalTime, true))
    else
      sendMessage(watermarkPublish, WatermarkTime(partitionID, finalTime, false))
  }

  override def name(): String = s"Reader $partitionID"
}
