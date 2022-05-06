package com.raphtory.config

import com.raphtory.components.Component
import com.raphtory.components.graphbuilder.BuilderExecutor
import com.raphtory.components.graphbuilder.GraphAlteration
import com.raphtory.components.graphbuilder.GraphBuilder
import com.raphtory.components.partition.BatchWriter
import com.raphtory.components.partition.LocalBatchHandler
import com.raphtory.components.partition.Reader
import com.raphtory.components.partition.StreamWriter
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.QueryManager
import com.raphtory.components.querytracker.QueryProgressTracker
import com.raphtory.components.spout.Spout
import com.raphtory.components.spout.SpoutExecutor
import com.raphtory.graph.GraphPartition
import com.raphtory.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Scheduler
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/** @DoNotDocument */
private[raphtory] class ComponentFactory(conf: Config, pulsarController: PulsarController) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  private val deploymentID     = conf.getString("raphtory.deploy.id")
  private val zookeeperAddress = conf.getString("raphtory.zookeeper.address")

  private val builderIDManager =
    new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/builderCount")

  private val partitionIDManager =
    new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")

  def builder[T: ClassTag](
      graphbuilder: GraphBuilder[T],
      batchLoading: Boolean = false,
      scheduler: Scheduler
  ): Option[List[ThreadedWorker[T]]] =
    if (!batchLoading) {
      val totalBuilders = conf.getInt("raphtory.builders.countPerServer")
      logger.info(s"Creating '$totalBuilders' Graph Builders.")

      logger.debug(s"Deployment ID set to '$deploymentID'.")

      val zookeeperAddress =
        logger.debug(s"Zookeeper Address set to '$deploymentID'.")

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
                  graphbuilder,
                  conf,
                  pulsarController
          )

        scheduler.execute(builderExecutor)
        ThreadedWorker(builderExecutor)
      }

      Some(builders.toList)
    }
    else None

  def partition[T: ClassTag](
      scheduler: Scheduler,
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
            throw new Exception(
                    s"Failed to retrieve Partition ID. " +
                      s"ID Manager at Zookeeper '$partitionIDManager' was unreachable."
            )
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

        val reader: Reader = new Reader(
                partitionID,
                storage,
                scheduler,
                conf,
                pulsarController
        )
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
    }
    else {
      val x: Seq[(GraphPartition, Reader, Component[GraphAlteration])] =
        for (i <- 0 until totalPartitions) yield {
          val partitionID = partitionIDManager.getNextAvailableID() match {
            case Some(id) => id
            case None     =>
              throw new Exception(
                      s"Failed to retrieve Partition ID. " +
                        s"ID Manager at Zookeeper '$partitionIDManager' was unreachable."
              )
          }

          val storage = new PojoBasedPartition(partitionID, conf)

          val writer =
            new StreamWriter(
                    partitionID,
                    storage,
                    conf,
                    pulsarController
            )
          scheduler.execute(writer)

          val reader = new Reader(
                  partitionID,
                  storage,
                  scheduler,
                  conf,
                  pulsarController
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
            s"Creating new Query Progress Tracker for '$jobID'."
    )

    val queryTracker =
      new QueryProgressTracker(jobID, conf, pulsarController)

    scheduler.execute(queryTracker)

    queryTracker
  }

  def stop(): Unit = {
    partitionIDManager.stop()
    builderIDManager.stop()
  }
}

case class ThreadedWorker[T](worker: Component[T])

case class Partitions(
    storages: List[GraphPartition],
    readers: List[Reader],
    writers: List[Component[GraphAlteration]]
)
