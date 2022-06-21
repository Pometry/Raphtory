package com.raphtory.internals.management

import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Spout
import com.raphtory.api.querytracker.QueryProgressTracker
import com.raphtory.internals.communication.TopicRepository
import com.raphtory.internals.components.Component
import com.raphtory.internals.components.graphbuilder.BuilderExecutor
import com.raphtory.internals.components.partition.BatchWriter
import com.raphtory.internals.components.partition.LocalBatchHandler
import com.raphtory.internals.components.partition.Reader
import com.raphtory.internals.components.partition.StreamWriter
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.QueryManager
import com.raphtory.internals.components.spout.SpoutExecutor
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphPartition
import com.raphtory.internals.management.id.LocalIDManager
import com.raphtory.internals.management.id.ZookeeperIDManager
import com.raphtory.internals.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag

private[raphtory] class ComponentFactory(
    conf: Config,
    topicRepo: TopicRepository,
    localDeployment: Boolean = false
) {
//  private val logger: Logger                = Logger(LoggerFactory.getLogger(this.getClass))
//  private lazy val deploymentID             = conf.getString("raphtory.deploy.id")
//  private lazy val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
//  private lazy val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
//  private lazy val totalPartitions: Int     = partitionServers * partitionsPerServer
//
//  private lazy val (builderIDManager, partitionIDManager) =
//    if (localDeployment)
//      (new LocalIDManager, new LocalIDManager)
//    else {
//      val zookeeperAddress     = conf.getString("raphtory.zookeeper.address")
//      val zkBuilderIDManager   = ZookeeperIDManager(zookeeperAddress, deploymentID, "builderCount")
//      val zkPartitionIDManager =
//        ZookeeperIDManager(zookeeperAddress, deploymentID, "partitionCount", totalPartitions)
//      (zkBuilderIDManager, zkPartitionIDManager)
//    }
//
//  def builder[T: ClassTag](
//      graphBuilder: GraphBuilder[T],
//      batchLoading: Boolean = false,
//      scheduler: Scheduler
//  ): Option[List[ThreadedWorker[T]]] =
//    if (!batchLoading) {
//      val totalBuilders = conf.getInt("raphtory.builders.countPerServer")
//      logger.info(s"Creating '$totalBuilders' Graph Builders.")
//
//      logger.debug(s"Deployment ID set to '$deploymentID'.")
//
//      val builders = for (name <- (0 until totalBuilders)) yield {
//        val builderId = builderIDManager
//          .getNextAvailableID()
//          .getOrElse(
//                  throw new Exception(
//                          s"Failed to retrieve Builder ID. " +
//                            s"ID Manager at Zookeeper '$builderIDManager' was unreachable."
//                  )
//          )
//
//        val builderExecutor =
//          new BuilderExecutor[T](
//                  builderId,
//                  deploymentID,
//                  graphBuilder,
//                  conf,
//                  topicRepo
//          )
//
//        scheduler.execute(builderExecutor)
//        ThreadedWorker(builderExecutor)
//      }
//
//      Some(builders.toList)
//    }
//    else None
//
//  def partition[T: ClassTag](
//      scheduler: Scheduler,
//      batchLoading: Boolean = false,
//      spout: Option[Spout[T]] = None,
//      graphBuilder: Option[GraphBuilder[T]] = None
//  ): Partitions = {
//    logger.info(s"Creating '$partitionsPerServer' Partition Managers for $deploymentID.")
//
//    val partitionIDs = List.fill(partitionsPerServer) {
//      partitionIDManager.getNextAvailableID() match {
//        case Some(id) => id
//        case None     => throw new Exception(s"Failed to retrieve Partition ID")
//      }
//    }
//
//    logger.info(s"Creating partitions with ids: $partitionIDs.")
//
//    val storages = partitionIDs map (id => new PojoBasedPartition(id, conf))
//
//    val readers = partitionIDs zip storages map {
//      case (id, storage) =>
//        new Reader(id, storage, scheduler, conf, topicRepo)
//    }
//
//    val writers =
//      if (batchLoading) {
//        val batchWriters      = partitionIDs zip storages map {
//          case (id, storage) =>
//            new BatchWriter[T](id, storage)
//        }
//        val localBatchHandler = new LocalBatchHandler[T](
//                mutable.Set.from(partitionIDs),
//                mutable.Map.from(partitionIDs zip batchWriters),
//                spout.get,
//                graphBuilder.get,
//                conf,
//                scheduler
//        )
//        List(localBatchHandler)
//      }
//      else
//        partitionIDs zip storages map {
//          case (id, storage) =>
//            new StreamWriter(id, storage, conf, topicRepo)
//        }
//
//    readers foreach (reader => scheduler.execute(reader))
//    writers foreach (writer => scheduler.execute(writer))
//
//    Partitions(storages, readers, writers)
//  }
//
//  def spout[T](
//      spout: Spout[T],
//      batchLoading: Boolean = false,
//      scheduler: Scheduler
//  ): Option[ThreadedWorker[T]] =
//    if (!batchLoading) {
//      val spoutExecutor = new SpoutExecutor[T](spout, conf, topicRepo, scheduler)
//      logger.info(s"Creating new Spout.")
//
//      scheduler.execute(spoutExecutor)
//      Some(ThreadedWorker(spoutExecutor))
//    }
//    else None
//
//  def query(scheduler: Scheduler): ThreadedWorker[QueryManagement] = {
//    logger.info(s"Creating new Query Manager.")
//
//    val queryManager = new QueryManager(scheduler, conf, topicRepo)
//    scheduler.execute(queryManager)
//    ThreadedWorker(queryManager)
//  }
//
//  def queryProgressTracker(
//      jobID: String,
//      scheduler: Scheduler
//  ): QueryProgressTracker = {
//    logger.info(
//            s"Creating new Query Progress Tracker for '$jobID'."
//    )
//
//    val queryTracker = new QueryProgressTracker(jobID, conf, topicRepo)
//    scheduler.execute(queryTracker)
//
//    queryTracker
//  }
//
//  def stop(): Unit = {
//    partitionIDManager.stop()
//    builderIDManager.stop()
//    topicRepo.shutdown()
//  }
}

private[raphtory] case class ThreadedWorker[T](worker: Component[T])

private[raphtory] case class Partitions(
    storages: List[GraphPartition],
    readers: List[Reader],
    writers: List[Component[GraphAlteration]]
)
