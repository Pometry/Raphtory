package com.raphtory.core.components.partition

import com.raphtory.core.components.Component
import com.raphtory.core.components.graphbuilder._
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.config.PulsarController
import com.typesafe.config.Config
import monix.execution.Scheduler

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.reflect.ClassTag

/** @DoNotDocument */
class LocalBatchHandler[T: ClassTag](
    partitionIDs: mutable.Set[Int],
    batchWriters: mutable.Map[Int, BatchWriter[T]],
    spout: Spout[T],
    graphBuilder: GraphBuilder[T],
    conf: Config,
    pulsarController: PulsarController,
    scheduler: Scheduler
) extends Component[GraphAlteration](conf: Config, pulsarController: PulsarController) {

  graphBuilder.setupBatchIngestion(partitionIDs, batchWriters, totalPartitions)

  private val rescheduler: Runnable = new Runnable {

    override def run(): Unit = {
      spout.executeReschedule()
      runIngestion()
    }
  }

  override def handleMessage(
      msg: GraphAlteration
  ): Unit = {} //No messages received by this component

  override def run(): Unit =
    runIngestion

  private def runIngestion(): Unit = {
    while (spout.hasNextIterator()) {
      startIngesting()
      spout.nextIterator().foreach(graphBuilder.parseTuple)
    }

    stopIngesting()
    if (spout.spoutReschedules())
      reschedule()
  }

  private def checkPartition(id: Long): Int =
    (id.abs % totalPartitions).toInt

  private def reschedule(): Unit = {
    // TODO: Parameterise the delay
    logger.debug("Spout: Scheduling spout to poll again in 10 seconds.")
    scheduler.scheduleOnce(10, TimeUnit.SECONDS, rescheduler)
  }

  private def startIngesting(): Unit =
    batchWriters.foreach {
      case (id, partition) => partition.getStorage().startBatchIngesting()
    }

  private def stopIngesting(): Unit =
    batchWriters.foreach {
      case (id, partition) => partition.getStorage().stopBatchIngesting()
    }

  override def stop(): Unit = {}
}
