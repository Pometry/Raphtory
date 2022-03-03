package com.raphtory.core.config

import com.raphtory.core.components.Component
import com.raphtory.core.components.graphbuilder.BuilderExecutor
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.partition.Reader
import com.raphtory.core.components.partition.Writer
import com.raphtory.core.components.querymanager.QueryManagement
import com.raphtory.core.components.querymanager.QueryManager
import com.raphtory.core.components.querytracker.QueryProgressTracker
import com.raphtory.core.components.spout.SpoutExecutor
import com.raphtory.core.storage.pojograph.PojoBasedPartition
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import monix.execution.Scheduler
import org.apache.pulsar.client.api.Schema
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/** @DoNotDocument */
private[core] class ComponentFactory(conf: Config, pulsarController: PulsarController) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def builder[T: ClassTag: TypeTag](
      graphbuilder: GraphBuilder[T],
      scheduler: Scheduler,
      schema: Schema[T]
  ): List[ThreadedWorker[T]] = {
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

    builders.toList
  }

  def partition(scheduler: Scheduler): List[Partition] = {
    val totalPartitions = conf.getInt("raphtory.partitions.countPerServer")
    logger.info(s"Creating '$totalPartitions' Partition Managers.")

    val deploymentID = conf.getString("raphtory.deploy.id")
    logger.debug(s"Deployment ID set to '$deploymentID'.")

    val zookeeperAddress = conf.getString("raphtory.zookeeper.address")
    logger.debug(s"Zookeeper Address set to '$deploymentID'.")

    val idManager = new ZookeeperIDManager(zookeeperAddress, s"/$deploymentID/partitionCount")

    val partitions = for (i <- 0 until totalPartitions) yield {
      val partitionID = idManager.getNextAvailableID() match {
        case Some(id) => id
        case None     =>
          throw new Exception(
                  s"Failed to retrieve Partition ID. " +
                    s"ID Manager at Zookeeper '$idManager' was unreachable."
          )
      }

      val storage = new PojoBasedPartition(partitionID, conf)
      val writer  = new Writer(partitionID, storage, conf, pulsarController)
      val reader  = new Reader(partitionID, storage, scheduler, conf, pulsarController)

      scheduler.execute(writer)
      scheduler.execute(reader)
      Partition(writer, reader)
    }

    partitions.toList
  }

  def spout[T](spout: SpoutExecutor[T], scheduler: Scheduler): ThreadedWorker[T] = {
    logger.info(s"Creating new Spout '${spout.spoutTopic}'.")

    scheduler.execute(spout)
    ThreadedWorker(spout)
  }

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

case class Partition(writer: Writer, reader: Reader)
case class ThreadedWorker[T](worker: Component[T])
