package com.raphtory.core.config

import com.raphtory.core.components.Component
import com.raphtory.core.components.graphbuilder.BuilderExecutor
import com.raphtory.core.components.graphbuilder.GraphBuilder
import com.raphtory.core.components.graphbuilder.GraphUpdate
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

import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag

private[core] class ComponentFactory(conf: Config, pulsarController: PulsarController) {
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  def builder[R: TypeTag: ClassTag](
      graphbuilder: GraphBuilder[R],
      scheduler: Scheduler
  ): List[ThreadedWorker[GraphUpdate, R]] = {
    val totalBuilders = conf.getInt("raphtory.builders.countPerServer")
    logger.info(s"Creating '$totalBuilders' Graph Builders.")

    val builders = for (i <- (0 until totalBuilders)) yield {
      val builderExecutor =
        new BuilderExecutor[R](graphbuilder, conf, pulsarController, scheduler)
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
      val writer  = new Writer(partitionID, storage, conf, pulsarController, scheduler)
      val reader  = new Reader(partitionID, storage, scheduler, conf, pulsarController)

      scheduler.execute(writer)
      scheduler.execute(reader)
      Partition(writer, reader)
    }

    partitions.toList
  }

  def spout[S](spout: SpoutExecutor[S], scheduler: Scheduler): ThreadedWorker[S, Nothing] = {
    logger.info(s"Creating new Spout '${spout.spoutTopic}'.")

    scheduler.execute(spout)
    ThreadedWorker[S, Nothing](spout)
  }

  def query(scheduler: Scheduler): ThreadedWorker[Long, QueryManagement] = {
    logger.info(s"Creating new Query Manager.")
    val queryManager = new QueryManager(scheduler, conf, pulsarController)

    scheduler.execute(queryManager)
    ThreadedWorker[Long, QueryManagement](queryManager)
  }

  def queryProgressTracker(
      topicId: String,
      deploymentID: String,
      jobID: String,
      scheduler: Scheduler
  ): QueryProgressTracker = {
    logger.info(
            s"Creating new Query Progress Tracker for deployment " +
              s"'$deploymentID' and job '$jobID' at topic '$topicId'."
    )

    val queryTracker =
      new QueryProgressTracker(topicId, deploymentID, jobID, scheduler, conf, pulsarController)

    scheduler.execute(queryTracker)
    ThreadedWorker[Nothing, QueryManagement](queryTracker)

    queryTracker
  }

}

case class Partition(writer: Writer, reader: Reader)
case class ThreadedWorker[S, R](worker: Component[S, R])
