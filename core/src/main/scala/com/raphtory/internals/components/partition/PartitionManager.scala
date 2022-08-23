package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.IO
import cats.effect.Resource
import cats.effect.Spawn
import cats.effect.unsafe.implicits.global
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.EstablishExecutor
import com.raphtory.internals.components.querymanager.GraphManagement
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.StopExecutor
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.util.concurrent.ConcurrentHashMap

class PartitionManager(
    graphID: String,
    partitionID: Int,
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository
) extends Component[GraphManagement](conf) {

  private val executors                    = new ConcurrentHashMap[String, QueryExecutor]()
  val storage                              = new PojoBasedPartition(graphID, partitionID, conf)
  val readerResource: Resource[IO, Reader] = Reader[IO](partitionID, storage, scheduler, conf, topics)
  val writerResource: Resource[IO, Writer] = Writer[IO](graphID, partitionID, storage, conf, topics)
  val (_, readerCancel)                    = readerResource.allocated.unsafeRunSync()
  val (_, writerCancel)                    = writerResource.allocated.unsafeRunSync()

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

      case IngestData(_, _, _)        => //this should never happen
    }

  override private[raphtory] def run(): Unit =
    logger.info(s"Partition $partitionID: Starting partition manager for '$graphID'.") // TODO: turn into debug

  override private[raphtory] def stop(): Unit = {
    executors forEach { (jobID, _) => Option(executors.remove(jobID)).foreach(_.stop()) }
    writerCancel.unsafeRunSync()
    readerCancel.unsafeRunSync()
  }

  private def establishExecutor(request: EstablishExecutor) =
    request match {
      case EstablishExecutor(_, graphID, jobID, sink, pyScript) =>
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
      topics: TopicRepository
  ): Resource[IO, PartitionManager] =
    Component.makeAndStartPart(
            partitionID,
            topics,
            s"partition-manager-$partitionID",
            List(topics.partitionSetup),
            new PartitionManager(graphID, partitionID, scheduler, conf, topics)
    )

}
