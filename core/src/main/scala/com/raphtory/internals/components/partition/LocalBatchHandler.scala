package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Resource
import cats.effect.Spawn
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.GraphBuilderInstance
import com.raphtory.api.input.Spout
import com.raphtory.api.input.SpoutInstance
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.management.Scheduler
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

private[raphtory] class LocalBatchHandler[T](
    partitionIDs: mutable.Set[Int],
    batchWriters: mutable.Map[Int, BatchWriter[T]],
    spout: SpoutInstance[T],
    graphBuilder: GraphBuilderInstance[T],
    conf: Config,
    scheduler: Scheduler
) extends Component[GraphAlteration](conf) {

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  var index: Long            = 0

  graphBuilder.setupBatchIngestion(partitionIDs, batchWriters, totalPartitions)

  // TODO get builderID to pull from zookeeper once stream and batch can run synchro

  private val rescheduler  = () => {
    spout.executeReschedule()
    runIngestion()
  }

  override def handleMessage(
      msg: GraphAlteration
  ): Unit = {} //No messages received by this component

  override def run(): Unit =
    runIngestion()

  private def runIngestion(): Unit = {
    while (spout.hasNextIterator()) {
      startIngesting()
      spout.nextIterator().foreach { line =>
        index += 1
        try graphBuilder.sendUpdates(line, index)(failOnError = true)
        catch {
          case e: Exception => logger.info(s"Could not parse: $line. Fails with exception ${e.toString}")
        }
      }
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
    scheduler.scheduleOnce(10.seconds, rescheduler())
  }

  private def startIngesting(): Unit =
    batchWriters.foreach {
      case (id, partition) =>
        partition.getStorage().startBatchIngesting()
    }

  private def stopIngesting(): Unit =
    batchWriters.foreach {
      case (id, partition) =>
        partition.getStorage().stopBatchIngesting()
    }
}

object LocalBatchHandler {

  def apply[IO[_]: Async: Spawn, T](
      deploymentID: String,
      partitionIds: mutable.Set[Int],
      batchWriters: mutable.Map[Int, BatchWriter[T]],
      spout: Spout[T],
      graphBuilder: GraphBuilder[T],
      topics: TopicRepository,
      config: Config,
      scheduler: Scheduler
  ): Resource[IO, Component[GraphAlteration]] =
    Component.makeAndStart[IO, GraphAlteration, LocalBatchHandler[T]](
            topics,
            s"local-batch-handler",
            Seq.empty,
            new LocalBatchHandler[T](
                    partitionIds,
                    batchWriters,
                    spout.buildSpout(),
                    graphBuilder.buildInstance(deploymentID),
                    config,
                    scheduler
            )
    )
}
