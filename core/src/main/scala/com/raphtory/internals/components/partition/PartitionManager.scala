package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.unsafe.implicits.global
import com.raphtory.api.output.sink.Sink
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.EstablishExecutor
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.GraphManagement
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
    graphID: String,
    partitionID: Int,
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository,
    batchLoading: Boolean,
    batchStorage: GraphPartition
) extends Component[GraphManagement](conf) {

  case class Partition(readerCancel: IO[Unit], writerCancel: IO[Unit], storage: GraphPartition) {

    def stop(): Unit = {
      readerCancel.unsafeRunSync()
      writerCancel.unsafeRunSync()
    }
  }

  private val executors                    = new ConcurrentHashMap[String, QueryExecutor]()
  val storage                              = new PojoBasedPartition(graphID, partitionID, conf)
  val readerResource: Resource[IO, Reader] = Reader[IO](partitionID, storage, scheduler, conf, topics)
  val writerResource: Resource[IO, Writer] = Writer[IO](graphID, partitionID, storage, conf, topics)
  val (_, readerCancel)                    = readerResource.allocated.unsafeRunSync()
  val (_, writerCancel)                    = writerResource.allocated.unsafeRunSync()
  val partition: Partition                 = Partition(readerCancel, writerCancel, storage)

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  override def handleMessage(msg: GraphManagement): Unit =
    msg match {
      case request: EstablishExecutor =>
        establishExecutor(request)

      case StopExecutor(jobID)        =>
        logger.debug(s"Partition manager $partitionID received EndQuery($jobID)")
        try Option(executors.remove(jobID)).foreach(_.stop())
        catch {
          case e: Exception =>
            e.printStackTrace()
        }
    }

  override private[raphtory] def run(): Unit =
    logger.info(s"Partition $partitionID: Starting partition manager for $graphID.") // TODO: turn into debug

  override private[raphtory] def stop(): Unit = {
    executors forEach { (jobID, _) => Option(executors.remove(jobID)).foreach(_.stop()) }
    partition.stop()
  }

  private def establishExecutor(request: EstablishExecutor) =
    request match {
      case EstablishExecutor(_, graphID, jobID, sink, pyScript) =>
        val storage       = if (batchLoading) batchStorage else partition.storage
        val queryExecutor =
          new QueryExecutor(partitionID, sink, storage, jobID, conf, topics, scheduler, pyScript)
        scheduler.execute(queryExecutor)
        executors.put(jobID, queryExecutor)
    }
}

object PartitionManager {

  def apply[IO[_]: Async: Spawn](
      graphID: String,
      partitionID: Int,
      scheduler: Scheduler,
      conf: Config,
      topics: TopicRepository,
      batchLoading: Boolean = false,
      batchStorage: GraphPartition = null // TODO: improve
  ): Resource[IO, PartitionManager] =
    Component.makeAndStartPart(
            partitionID,
            topics,
            s"partition-manager-$partitionID",
            List(topics.partitionSetup),
            new PartitionManager(graphID, partitionID, scheduler, conf, topics, batchLoading, batchStorage)
    )

}
