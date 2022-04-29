package com.raphtory.components.partition

import com.raphtory.components.Component
import com.raphtory.components.graphbuilder._
import com.raphtory.components.spout.Spout
import com.raphtory.config.PulsarController
import com.raphtory.config.telemetry.BuilderTelemetry
import com.raphtory.config.telemetry.PartitionTelemetry
import com.raphtory.serialisers.Marshal
import com.typesafe.config.Config
import io.prometheus.client.Counter
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

  private val vertexAddCounter    = BuilderTelemetry.totalVertexAdds(deploymentID)
  private val vertexDeleteCounter = BuilderTelemetry.totalVertexDeletes(deploymentID)
  private val edgeAddCounter      = BuilderTelemetry.totalEdgeAdds(deploymentID)
  private val edgeDeleteCounter   = BuilderTelemetry.totalEdgeDeletes(deploymentID)

  graphBuilder.setupBatchIngestion(partitionIDs, batchWriters, totalPartitions)

  // TODO get builderID to pull from zookeeper once stream and batch can run synchro
  graphBuilder.setBuilderMetaData(
          builderID = 0,
          deploymentID,
          vertexAddCounter,
          vertexDeleteCounter,
          edgeAddCounter,
          edgeDeleteCounter
  )

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
      case (id, partition) =>
        partition.getStorage().startBatchIngesting()
        PartitionTelemetry.timeForIngestion(id).startTimer()
    }

  private def stopIngesting(): Unit =
    batchWriters.foreach {
      case (id, partition) =>
        partition.getStorage().stopBatchIngesting()
        PartitionTelemetry.timeForIngestion(id).startTimer().setDuration()
    }

  override def stop(): Unit = {}
}
