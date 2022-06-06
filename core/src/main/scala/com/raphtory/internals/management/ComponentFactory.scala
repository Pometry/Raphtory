package com.raphtory.internals.management

import com.raphtory.api.input.GraphAlteration
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.graphbuilder.BuilderExecutor
import com.raphtory.internals.components.partition.BatchWriter
import com.raphtory.internals.components.partition.LocalBatchHandler
import com.raphtory.internals.components.partition.Reader
import com.raphtory.internals.components.partition.StreamWriter
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.querytracker.QueryProgressTracker
import com.raphtory.internals.components.spout.SpoutExecutor
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.internals.management.id.ZookeeperIDManager
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag

/** @note DoNotDocument */
private[raphtory] class ComponentFactory(
    conf: Config,
    topicRepo: TopicRepository,
    localDeployment: Boolean = false
) {
  private val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private lazy val deploymentID = conf.getString("raphtory.deploy.id")

  private lazy val (builderIDManager, partitionIDManager) =
    if (localDeployment)
      (new LocalIDManager, new LocalIDManager)
    else {
      val zookeeperAddress     = conf.getString("raphtory.zookeeper.address")
      val zkBuilderIDManager   =
        new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/builderCount")
      val zkPartitionIDManager =
        new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")
      (zkBuilderIDManager, zkPartitionIDManager)
    }

  def builder[T: ClassTag](
      graphBuilder: GraphBuilder[T],
      batchLoading: Boolean = false,
      scheduler: MonixScheduler
  ): Option[List[ThreadedWorker[T]]] =
    if (!batchLoading) {
      val totalBuilders = conf.getInt("raphtory.builders.countPerServer")
      logger.info(s"Creating '$totalBuilders' Graph Builders.")

      logger.debug(s"Deployment ID set to '$deploymentID'.")

      val builders = for (name <- (0 until totalBuilders)) yield {
        val builderId = builderIDManager
          .getNextAvailableID()
          .getOrElse(
                  throw new Exception(
                          s"Failed to retrieve Builder ID. " +
                            s"ID Manager at Zookeeper '$builderIDManager' was unreachable."
                  )
          )

        val builderExecutor =
          new BuilderExecutor[T](
                  builderId,
                  deploymentID,
                  graphBuilder,
                  conf,
                  topicRepo
          )

        scheduler.execute(builderExecutor)
        ThreadedWorker(builderExecutor)
      }

      Some(builders.toList)
    }
    else None

  def partition[T: ClassTag](
      scheduler: MonixScheduler,
      batchLoading: Boolean = false,
      spout: Option[Spout[T]] = None,
      graphBuilder: Option[GraphBuilder[T]] = None
  ): Partitions = {
    val totalPartitions = conf.getInt("raphtory.partitions.countPerServer")
    logger.info(s"Creating '$totalPartitions' Partition Managers for $deploymentID.")

    val batchWriters = mutable.Map[Int, BatchWriter[T]]()
    val partitionIDs = mutable.Set[Int]()

    val partitions = if (batchLoading) {
      val x: Seq[(GraphPartition, Reader)] = for (i <- 0 until totalPartitions) yield {
        val partitionID = partitionIDManager.getNextAvailableID() match {
          case Some(id) => id
          case None     =>
            throw new Exception(s"Failed to retrieve Partition ID")
        }

        val storage: GraphPartition = new PojoBasedPartition(partitionID, conf)

        batchWriters += (
                (
                        i,
                        new BatchWriter[T](
                                partitionID,
                                storage
                        )
                )
        )
        partitionIDs += i

        val reader: Reader = new Reader(partitionID, storage, scheduler, conf, topicRepo)
        scheduler.execute(reader)

        (storage, reader)
      }

      val batchHandler = new LocalBatchHandler[T](
              partitionIDs,
              batchWriters,
              spout.get,
              graphBuilder.get,
              conf,
              scheduler
      )
      scheduler.execute(batchHandler)

      Partitions(x.map(_._1).toList, x.map(_._2).toList, List(batchHandler))
    }
    else {
      val x: Seq[(GraphPartition, Reader, Component[GraphAlteration])] =
        for (i <- 0 until totalPartitions) yield {
          val partitionID = partitionIDManager.getNextAvailableID() match {
            case Some(id) => id
            case None     =>
              throw new Exception(s"Failed to retrieve Partition ID")
          }

          val storage = new PojoBasedPartition(partitionID, conf)

          val writer =
            new StreamWriter(
                    partitionID,
                    storage,
                    conf,
                    topicRepo
            )
          scheduler.execute(writer)

          val reader = new Reader(
                  partitionID,
                  storage,
                  scheduler,
                  conf,
                  topicRepo
          )
          scheduler.execute(reader)

          (storage, reader, writer)
        }

      Partitions(x.map(_._1).toList, x.map(_._2).toList, x.map(_._3).toList)
    }

    partitions
  }

  def spout[T](
      spout: Spout[T],
      batchLoading: Boolean = false,
      scheduler: MonixScheduler
  ): Option[ThreadedWorker[T]] =
    if (!batchLoading) {
      val spoutExecutor = new SpoutExecutor[T](spout, conf, topicRepo, scheduler)
      logger.info(s"Creating new Spout.")

      scheduler.execute(spoutExecutor)
      Some(ThreadedWorker(spoutExecutor))
    }
    else None

  def query(scheduler: MonixScheduler): ThreadedWorker[QueryManagement] = {
    logger.info(s"Creating new Query Manager.")

    val queryManager = new QueryManager(scheduler, conf, topicRepo)
    scheduler.execute(queryManager)
    ThreadedWorker(queryManager)
  }

  def queryProgressTracker(
      jobID: String,
      scheduler: MonixScheduler
  ): QueryProgressTracker = {
    logger.info(
            s"Creating new Query Progress Tracker for '$jobID'."
    )

    val queryTracker = new QueryProgressTracker(jobID, conf, topicRepo)
    scheduler.execute(queryTracker)

    queryTracker
  }

  def stop(): Unit = {
    partitionIDManager.stop()
    builderIDManager.stop()
    topicRepo.shutdown()
  }
}

case class ThreadedWorker[T](worker: Component[T])

case class Partitions(
    storages: List[GraphPartition],
    readers: List[Reader],
    writers: List[Component[GraphAlteration]]
)
