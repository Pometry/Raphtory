package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.EstablishExecutor
import com.raphtory.internals.components.querymanager.EstablishPartition
import com.raphtory.internals.components.querymanager.PartitionManagement
import com.raphtory.internals.components.querymanager.StopExecutor
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

class PartitionManager(
    partitionID: Int,
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[PartitionManagement](conf) {

  case class Partition(readerCancel: IO[Unit], writerCancel: IO[Unit], storage: GraphPartition) {

    def stop(): Unit = {
      readerCancel.unsafeRunSync()
      writerCancel.unsafeRunSync()
    }
  }

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val partitions = mutable.Map[String, Partition]()
  private val executors  = new ConcurrentHashMap[String, QueryExecutor]()

  override def handleMessage(msg: PartitionManagement): Unit =
    msg match {
      case EstablishPartition(graphID)                =>
        val storage           = new PojoBasedPartition(partitionID, conf)
        val readerResource    = Reader[IO](graphID, partitionID, storage, scheduler, conf, topics)
        val writerResource    = StreamWriter[IO](graphID, partitionID, storage, conf, topics)
        val (_, readerCancel) = readerResource.allocated.unsafeRunSync()
        val (_, writerCancel) = writerResource.allocated.unsafeRunSync()
        partitions.put(graphID, Partition(readerCancel, writerCancel, storage))
      case EstablishExecutor(_, graphID, jobID, sink) =>
        val storage       = partitions(graphID).storage
        val queryExecutor =
          new QueryExecutor(partitionID, sink, storage, graphID, jobID, conf, topics, scheduler)
        scheduler.execute(queryExecutor)
//        telemetry.queryExecutorCollector.labels(partitionID.toString, deploymentID).inc()
        executors.put(jobID, queryExecutor)
      case StopExecutor(jobID)                        =>
        logger.debug(s"Partition manager $partitionID received EndQuery($jobID)")
        try Option(executors.remove(jobID)).foreach(_.stop())
//            telemetry.queryExecutorCollector.labels(partitionID.toString, deploymentID).dec()
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
    }

  override private[raphtory] def run(): Unit =
    logger.info(s"Partition $partitionID: Starting partition manager.") // TODO: turn into debug

  override private[raphtory] def stop(): Unit = {
    executors forEach { (jobID, _) => Option(executors.remove(jobID)).foreach(_.stop()) }
    partitions foreach { case (graphID, _) => partitions.remove(graphID).foreach(_.stop()) }
  }
}

object PartitionManager {

  def apply[IO[_]: Async: Spawn](
      partitionID: Int,
      scheduler: Scheduler,
      conf: Config,
      topics: TopicRepository
  ): Resource[IO, PartitionManager] =
    Component.makeAndStartPart(
            partitionID,
            topics,
            s"partition-manager-$partitionID",
            List(topics.partitionSetup),
            new PartitionManager(partitionID, scheduler, conf, topics)
    )

}
