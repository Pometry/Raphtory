package com.raphtory.internals.components.partition

import cats.effect.Async
import cats.effect.Resource
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.querymanager.EstablishExecutor
import com.raphtory.internals.components.querymanager.GraphManagement
import com.raphtory.internals.components.querymanager.StopExecutor
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.Scheduler
import com.raphtory.internals.storage.arrow.ArrowPartition
import com.raphtory.internals.storage.arrow.ArrowPartitionConfig
import com.raphtory.internals.storage.arrow.ArrowSchema
import com.raphtory.internals.storage.arrow.immutable
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap

class PartitionManager(
    graphID: String,
    partitionID: Int,
    scheduler: Scheduler,
    conf: Config,
    topics: TopicRepository,
    storage: GraphPartition
) extends Component[GraphManagement](conf) {

  private val executors = new ConcurrentHashMap[String, QueryExecutor]()

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
    logger.info(s"Partition $partitionID: Starting partition manager for '$graphID'.") // TODO: turn into debug

  override private[raphtory] def stop(): Unit =
    executors forEach { (jobID, _) => Option(executors.remove(jobID)).foreach(_.stop()) }

  private def establishExecutor(request: EstablishExecutor) =
    request match {
      case EstablishExecutor(_, graphID, jobID, sink, pyScript) =>
        val queryExecutor =
          new QueryExecutor(graphID, partitionID, sink, storage, jobID, conf, topics, scheduler, pyScript)
        scheduler.execute(queryExecutor)
        executors.put(jobID, queryExecutor)
    }
}

object PartitionManager {

  def apply[IO[_]](graphID: String, partitionID: Int, scheduler: Scheduler, conf: Config, topics: TopicRepository)(
      implicit IO: Async[IO]
  ): Resource[IO, PartitionManager] =
    Resource.eval(IO.delay(new PojoBasedPartition(graphID, partitionID, conf))).flatMap { storage =>
      fromStorage(storage, graphID, partitionID, scheduler, conf, topics)
    }

  def fromStorage[IO[_]](
      storage: GraphPartition,
      graphID: String,
      partitionID: Int,
      scheduler: Scheduler,
      conf: Config,
      topics: TopicRepository
  )(implicit IO: Async[IO]): Resource[IO, PartitionManager] =
    for {

      _  <- Reader[IO](graphID, partitionID, storage, scheduler, conf, topics)
      _  <- Writer[IO](graphID, partitionID, storage, conf, topics, scheduler)
      pm <- Component.makeAndStartPart(
                    partitionID,
                    topics,
                    s"partition-manager-$partitionID",
                    List(topics.partitionSetup(graphID)),
                    new PartitionManager(
                            graphID,
                            partitionID,
                            scheduler,
                            conf,
                            topics,
                            storage
                    )
            )
    } yield pm

  def arrow[IO[_]](
      graphID: String,
      partitionID: Int,
      scheduler: Scheduler,
      conf: Config,
      topics: TopicRepository
  )(implicit IO: Async[IO]): Resource[IO, PartitionManager] =
    Resource
      .eval(arrowPartition(graphID, partitionID, conf))
      .flatMap(storage => fromStorage(storage, graphID, partitionID, scheduler, conf, topics))

  /**
    * Creates an arrow partition
    * @param graphID
    * @param partitionID
    * @param conf
    * @param IO
    * @tparam IO
    * @return
    */
  private def arrowPartition[IO[_]](graphID: String, partitionID: Int, conf: Config)(implicit IO: Async[IO]) =
    IO.blocking(
            ArrowPartition(
                    graphID,
                    ArrowPartitionConfig(
                            conf,
                            partitionID,
                            ArrowSchema[NodeSchema, EdgeSchema],
                            Files.createTempDirectory("experimental")
                    ),
                    conf
            )
    )
}

case class NodeSchema(@immutable name: String)
case class EdgeSchema(weight: Long)
