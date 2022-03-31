package com.raphtory.core.config

import com.raphtory.core.components.Component
import com.raphtory.core.components.graphbuilder.BuilderExecutor
import com.raphtory.core.components.graphbuilder.GraphAlteration
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.partition.BatchWriter
import com.raphtory.core.components.partition.LocalBatchHandler
import com.raphtory.core.components.partition.Reader
import com.raphtory.core.components.partition.StreamWriter
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.QueryManager
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.components.spout.Spout
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.graph.GraphPartition
import com.raphtory.core.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/** @DoNotDocument */
private[core] class ComponentFactory(conf: Config, pulsarController: PulsarController) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))
  val builderIDs: List[String] = List()

  def builder[T: ClassTag](
                            graphbuilder: GraphBuilder[T],
                            batchLoading: Boolean = false,
                            scheduler: Scheduler
                          ): Option[List[ThreadedWorker[T]]] =
    if (!batchLoading) {
      val totalBuilders = conf.getInt("raphtory.builders.countPerServer")
      logger.info(s"Creating '$totalBuilders' Graph Builders.")

      val deploymentID = conf.getString("raphtory.deploy.id")
      logger.debug(s"Deployment ID set to '$deploymentID'.")

      val zookeeperAddress = conf.getString("raphtory.zookeeper.address")
      logger.debug(s"Zookeeper Address set to '$deploymentID'.")

      val idManager = new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/builderCount")

      val builders = for (name <- (0 until totalBuilders)) yield {
        val builderId = idManager
          .getNextAvailableID()
          .getOrElse(
            throw new Exception(
              s"Failed to retrieve Builder ID. " +
                s"ID Manager at Zookeeper '$idManager' was unreachable."
            )
          )

        val builderExecutor =
          new BuilderExecutor[T](builderId.toString, graphbuilder, conf, pulsarController)

        scheduler.execute(builderExecutor)
        ThreadedWorker(builderExecutor)
      }

      Some(builders.toList)
    }
    else None

  //  def partition[T: ClassTag](
  //      scheduler: Scheduler,
  //      batchLoading: Boolean = false,
  //      spout: Option[Spout[T]] = None,
  //      graphBuilder: Option[GraphBuilder[T]] = None
  //  ): List[GraphPartition] = {
  //    val totalPartitions = conf.getInt("raphtory.partitions.countPerServer")
  //    logger.info(s"Creating '$totalPartitions' Partition Managers.")
  //
  //    val deploymentID = conf.getString("raphtory.deploy.id")
  //    logger.debug(s"Deployment ID set to '$deploymentID'.")
  //
  //    val zookeeperAddress = conf.getString("raphtory.zookeeper.address")
  //    logger.debug(s"Zookeeper Address set to '$deploymentID'.")
  //
  //    val idManager = new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")
  //
  //    val batchWriters = mutable.Map[Int, BatchWriter[T]]()
  //    val partitionIDs = mutable.Set[Int]()
  //
  //    val storages = for (i <- 0 until totalPartitions) yield {
  //      val partitionID = idManager.getNextAvailableID() match {
  //        case Some(id) => id
  //        case None     =>
  //          throw new Exception(
  //                  s"Failed to retrieve Partition ID. " +
  //                    s"ID Manager at Zookeeper '$idManager' was unreachable."
  //          )
  //      }
  //
  //      // TODO Arrow WIP
  //      val storage = if (FeatureToggles.isArrowEnabled) {
  //        new ArrowGraphPartition(partitionID, conf)
  //      } else new PojoBasedPartition(partitionID, conf)
  //
  //      if (batchLoading) {
  //        batchWriters += (
  //                (
  //                        i,
  //                        new BatchWriter[T](
  //                                partitionID,
  //                                storage
  //                        )
  //                )
  //        )
  //        partitionIDs += i
  //      }
  //      else {
  //        val writer = new StreamWriter(partitionID, storage, conf, pulsarController)
  //        scheduler.execute(writer)
  //      }
  //
  //      val reader = new Reader(partitionID, storage, scheduler, conf, pulsarController)
  //      scheduler.execute(reader)
  //
  //      storage
  //    }
  //
  //    if (batchLoading) {
  //      val batchHandler = new LocalBatchHandler[T](
  //              partitionIDs,
  //              batchWriters,
  //              spout.get,
  //              graphBuilder.get,
  //              conf,
  //              pulsarController,
  //              scheduler
  //      )
  //      scheduler.execute(batchHandler)
  //    }
  //
  //    storages.toList
  //  }

  def partition[T: ClassTag](
                              scheduler: Scheduler,
                              batchLoading: Boolean = false,
                              spout: Option[Spout[T]] = None,
                              graphBuilder: Option[GraphBuilder[T]] = None
                            ): Partitions = {
    val totalPartitions = conf.getInt("raphtory.partitions.countPerServer")
    logger.info(s"Creating '$totalPartitions' Partition Managers.")

    val deploymentID = conf.getString("raphtory.deploy.id")
    logger.debug(s"Deployment ID set to '$deploymentID'.")

    val zookeeperAddress = conf.getString("raphtory.zookeeper.address")
    logger.debug(s"Zookeeper Address set to '$deploymentID'.")

    val idManager = new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")

    val batchWriters = mutable.Map[Int, BatchWriter[T]]()
    val partitionIDs = mutable.Set[Int]()

    val partitions = if(batchLoading) {
      val x: Seq[(GraphPartition, Reader)] = for (i <- 0 until totalPartitions) yield {
        val partitionID = idManager.getNextAvailableID() match {
          case Some(id) => id
          case None =>
            throw new Exception(
              s"Failed to retrieve Partition ID. " +
                s"ID Manager at Zookeeper '$idManager' was unreachable."
            )
        }

        val storage: GraphPartition = new PojoBasedPartition(partitionID, conf)

        batchWriters += ((
          i,
          new BatchWriter[T](
            partitionID,
            storage
          )
        ))
        partitionIDs += i

        val reader: Reader = new Reader(partitionID, storage, scheduler, conf, pulsarController)
        scheduler.execute(reader)

        (storage, reader)
      }

      val batchHandler = new LocalBatchHandler[T](
        partitionIDs,
        batchWriters,
        spout.get,
        graphBuilder.get,
        conf,
        pulsarController,
        scheduler
      )
      scheduler.execute(batchHandler)

      Partitions(x.map(_._1).toList, x.map(_._2).toList, List(batchHandler))
    } else {
      val x: Seq[(GraphPartition, Reader, Component[GraphAlteration])] =
        for (i <- 0 until totalPartitions) yield {
          val partitionID = idManager.getNextAvailableID() match {
            case Some(id) => id
            case None     =>
              throw new Exception(
                s"Failed to retrieve Partition ID. " +
                  s"ID Manager at Zookeeper '$idManager' was unreachable."
              )
          }

          val storage = new PojoBasedPartition(partitionID, conf)

          val writer = new StreamWriter(partitionID, storage, conf, pulsarController)
          scheduler.execute(writer)

          val reader = new Reader(partitionID, storage, scheduler, conf, pulsarController)
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
                scheduler: Scheduler
              ): Option[ThreadedWorker[T]] =
    if (!batchLoading) {
      val spoutExecutor = new SpoutExecutor[T](spout, conf, pulsarController, scheduler)
      logger.info(s"Creating new Spout '${spoutExecutor.spoutTopic}'.")

      scheduler.execute(spoutExecutor)
      Some(ThreadedWorker(spoutExecutor))
    }
    else None

  def query(scheduler: Scheduler): ThreadedWorker[QueryManagement] = {
    logger.info(s"Creating new Query Manager.")
    val queryManager = new QueryManager(scheduler, conf, pulsarController)

    scheduler.execute(queryManager)
    ThreadedWorker(queryManager)
  }

  def queryProgressTracker(
                            jobID: String,
                            scheduler: Scheduler
                          ): QueryProgressTracker = {
    logger.info(
      s"Creating new Query Progress Tracker for  '$jobID'."
    )

    val queryTracker =
      new QueryProgressTracker(jobID, conf, pulsarController)

    scheduler.execute(queryTracker)
    ThreadedWorker(queryTracker)

    queryTracker
  }

}

case class ThreadedWorker[T](worker: Component[T])

case class Partitions(storages: List[GraphPartition], readers: List[Reader], writers: List[Component[GraphAlteration]])
