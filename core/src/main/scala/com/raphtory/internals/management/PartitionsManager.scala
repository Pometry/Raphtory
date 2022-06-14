package com.raphtory.internals.management

import cats.effect
import cats.effect.Async
import cats.effect.kernel.Resource
import cats.effect.kernel.Spawn
import com.raphtory.api.input.GraphAlteration
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.partition.BatchWriter
import com.raphtory.internals.components.partition.LocalBatchHandler
import com.raphtory.internals.components.partition.Reader
import com.raphtory.internals.components.partition.StreamWriter
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.id.IDManager
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag

sealed trait PartitionsManager

case class BatchPartitionManager[T](
    storages: List[GraphPartition],
    readers: List[Reader],
    writers: List[Component[GraphAlteration]],
    partitionIds: List[Int],
    batchWriters: List[(Int, BatchWriter[T])]
) extends PartitionsManager

object BatchPartitionManager {

  def empty[T]: BatchPartitionManager[T] =
    BatchPartitionManager(List.empty, List.empty, List.empty, List.empty, List.empty)
}

case class StreamingPartitionManager(
    storages: List[GraphPartition],
    readers: List[Reader],
    writers: List[Component[GraphAlteration]]
) extends PartitionsManager

object StreamingPartitionManager {
  def empty = StreamingPartitionManager(List.empty, List.empty, List.empty)
}

object PartitionsManager {
  import alleycats.std.iterable._
  import cats.syntax.all._

  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def batchLoading[IO[_]: Spawn, T: ClassTag](
      config: Config,
      partitionIDManager: IDManager,
      topics: TopicRepository,
      scheduler: Scheduler,
      spout: Spout[T],
      graphBuilder: GraphBuilder[T]
  )(implicit IO: Async[IO]): Resource[IO, BatchPartitionManager[T]] = {

    val deploymentID    = config.getString("raphtory.deploy.id")
    val totalPartitions = config.getInt("raphtory.partitions.countPerServer")

    logger.info(s"Creating '$totalPartitions' Partition Managers for $deploymentID.")

    val partitions: Iterable[Int] = 0 until totalPartitions

    val partMResource: Resource[IO, BatchPartitionManager[T]] = partitions.foldLeftM(BatchPartitionManager.empty[T]) {
      (pm, i) =>
        val pm1 = for {
          partitionId <- nextId(partitionIDManager)
          storage     <- IO.delay(new PojoBasedPartition(partitionId, config))
          writer      <- IO.delay(new BatchWriter[T](partitionId, storage))
        } yield (
                partitionId,
                pm.copy(
                        batchWriters = (partitionId, writer) :: pm.batchWriters,
                        partitionIds = partitionId :: pm.partitionIds
                ),
                storage
        )
        for {
          a1 <- cats.effect.Resource.eval(pm1)
          (partitionId, pm, storage) = a1
          reader                    <- Reader(partitionId, storage, scheduler, config, topics)
        } yield pm.copy(readers = reader :: pm.readers)
    }

    for {
      pm           <- partMResource
      batchHandler <- LocalBatchHandler(
                              mutable.Set(pm.partitionIds: _*),
                              mutable.Map(pm.batchWriters: _*),
                              spout,
                              graphBuilder,
                              topics,
                              config,
                              scheduler
                      )
    } yield pm.copy(writers = batchHandler :: pm.writers)

  }

  def nextId[IO[_]: Async](partitionIDManager: IDManager): IO[Int] =
    Async[IO].blocking {
      partitionIDManager.getNextAvailableID() match {
        case Some(id) => id
        case None     =>
          throw new Exception(s"Failed to retrieve Partition ID")
      }
    }

  def streaming[IO[_]: Spawn, T](
      config: Config,
      partitionIDManager: IDManager,
      topics: TopicRepository,
      scheduler: Scheduler
  )(implicit
      IO: Async[IO]
  ): Resource[IO, PartitionsManager] = {

    val deploymentID    = config.getString("raphtory.deploy.id")
    val totalPartitions = config.getInt("raphtory.partitions.countPerServer")

    logger.info(s"Creating '$totalPartitions' Partition Managers for $deploymentID.")

    val partitions: Iterable[Int] = 0 until totalPartitions

    val partMResource: Resource[IO, StreamingPartitionManager] = partitions.foldLeftM(StreamingPartitionManager.empty) {
      (pm, i) =>
        val pm1 = for {
          partitionId <- nextId(partitionIDManager)
          storage     <- IO.delay(new PojoBasedPartition(partitionId, config))
        } yield (partitionId, storage)
        for {
          a1 <- cats.effect.Resource.eval(pm1)
          (partitionId, storage) = a1
          reader                <- Reader(partitionId, storage, scheduler, config, topics)
          writer                <- StreamWriter(partitionId, storage, config, topics)
        } yield pm.copy(readers = reader :: pm.readers, writers = writer :: pm.writers)
    }

    partMResource
  }
}
