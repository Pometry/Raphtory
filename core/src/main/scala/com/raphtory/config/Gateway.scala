package com.raphtory.config

import com.raphtory.components.Component
import com.raphtory.components.graphbuilder.BuilderExecutor
import com.raphtory.components.graphbuilder.GraphAlteration
import com.raphtory.components.graphbuilder.GraphUpdate
import com.raphtory.components.graphbuilder.GraphUpdateEffect
import com.raphtory.components.partition.BatchWriter
import com.raphtory.components.partition.QueryExecutor
import com.raphtory.components.partition.Reader
import com.raphtory.components.partition.StreamWriter
import com.raphtory.components.querymanager.EndQuery
import com.raphtory.components.querymanager.GenericVertexMessage
import com.raphtory.components.querymanager.Query
import com.raphtory.components.querymanager.QueryHandler
import com.raphtory.components.querymanager.QueryManagement
import com.raphtory.components.querymanager.QueryManager
import com.raphtory.components.querymanager.WatermarkTime
import com.raphtory.components.querytracker.QueryProgressTracker
import com.raphtory.graph.Perspective
import com.typesafe.config.Config

import scala.reflect.ClassTag

abstract class Gateway(defaultConnector: Connector, conf: Config) {

  // Methods to override:
  protected def connectors[T]: Map[Topic[T], Connector] = Map()

  // Topic definitions
  sealed trait Topic[T] {
    type MsgType = T
    def baseAddress: String

    def address(jobId: Option[String], partition: Option[Int]): String = {
      val jobIdSuffix     = if (jobId.isDefined) s"-$jobId" else ""
      val partitionSuffix = if (partition.isDefined) s"-$partition" else ""
      s"$baseAddress$jobIdSuffix$partitionSuffix"
    }
  }

  case class Exclusive[T](baseAddress: String)                 extends Topic[T]
  case class Broadcast[T](baseAddress: String)                 extends Topic[T]
  case class WorkPull[T](baseAddress: String)                  extends Topic[T]
  case class Sharding[T](baseAddress: String, partitions: Int) extends Topic[T]

  // Configuration
  val spoutAddress: String     = conf.getString("raphtory.spout.topic")
  val depId: String            = conf.getString("raphtory.deploy.id")
  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  protected val spoutOutput  = WorkPull[Any](spoutAddress)
  protected val graphUpdates = Sharding[GraphUpdate](s"graph-updates-$depId", numPartitions)
  protected val graphSync    = Sharding[GraphUpdateEffect](s"graph-sync-$depId", numPartitions)
  protected val queries      = Exclusive[Query](s"queries-$depId")
  protected val endedQueries = Exclusive[EndQuery](s"ended-queries-$depId")
  protected val watermark    = Exclusive[WatermarkTime](s"watermark-$depId")
  protected val queryPrep    = Broadcast[QueryManagement](s"query-prep-$depId")

  // Job wise topics
  protected val queryTrack     = Exclusive[QueryManagement](s"query-track-$depId")
  protected val rechecks       = Exclusive[QueryManagement](s"recheck-$depId")
  protected val jobStatus      = Exclusive[QueryManagement](s"job-status-$depId")
  protected val vertexMessages = Sharding[GenericVertexMessage](s"vertex-msg-$depId", numPartitions)
  protected val jobOperations  = Broadcast[QueryManagement](s"job-ops-$depId")

  // EndPoint getters
  private type gu = graphUpdates.MsgType
  private type gs = graphSync.MsgType
  def toSpoutOutput                    = createEndPoint(spoutOutput)
  def toGraphUpdates(shard: gu => Int) = createEndPoint(graphUpdates, None, Some(shard))
  def toGraphSync(shard: gs => Int)    = createEndPoint(graphSync, None, Some(shard))
  def toQueries                        = createEndPoint(queries)
  def toEndedQueries                   = createEndPoint(endedQueries)
  def toWatermark                      = createEndPoint(watermark)
  def toQueryPrep                      = createEndPoint(queryPrep)

  private type vm = vertexMessages.MsgType
  def toQueryTrack(jobId: String) = createEndPoint(queryTrack)
  def toRechecks(jobId: String)   = createEndPoint(rechecks, Some(jobId))
  def toJobStatus(jobId: String)  = createEndPoint(jobStatus, Some(jobId))

  def toVertexMessages(jobId: String, shard: vm => Int) =
    createEndPoint(vertexMessages, Some(jobId), Some(shard))
  def toJobOperations(jobId: String)                    = createEndPoint(jobOperations, Some(jobId))

  // Registering functions
  def registerBuilderExecutor(component: BuilderExecutor[Any]): Unit =
    register(component, spoutOutput)

  def registerStreamWriter(component: StreamWriter, partition: Int): Unit =
    register(component, Seq(graphUpdates, graphSync), None, Some(partition))

  def registerReader(component: Reader, partition: Int): Unit =
    register(component, queryPrep, None, Some(partition))

  def registerQueryManager(component: QueryManager): Unit =
    register(component, Seq(queries, endedQueries, watermark))

  def registerQueryProgressTracker(component: QueryProgressTracker, jobId: String): Unit =
    register(component, queryTrack, Some(jobId))

  def registerQueryHandler(component: QueryHandler, jobId: String): Unit =
    register(component, Seq(rechecks, jobStatus), Some(jobId))

  def registerQueryExecutor(component: QueryExecutor, jobId: String, partition: Int): Unit =
    register(component, Seq(vertexMessages, jobOperations), Some(jobId), Some(partition))

  private def createEndPoint[T](
      topic: Topic[T],
      jobId: Option[String] = None,
      sharding: Option[T => Int] = None
  ) = {
    val connector = connectors.getOrElse(topic, defaultConnector)
    topic match {
      case _: Exclusive[T] =>
        connector.createExclusiveEndPoint[T](topic.address(jobId, None))
      case _: WorkPull[T]  =>
        connector.createWorkPullEndPoint[T](topic.address(jobId, None))
      case _: Broadcast[T] =>
        connector.createBroadcastEndPoint[T](topic.address(jobId, None))
      case _: Sharding[T]  =>
        new EndPoint[T] {
          private val partitions                   = 0 until numPartitions
          private val endPoints                    = partitions map { partition =>
            connector.createExclusiveEndPoint[T](topic.address(jobId, Some(partition)))
          }
          override def sendAsync(message: T): Unit =
            endPoints(sharding.get.apply(message)) sendAsync message
          override def close(): Unit               = endPoints foreach (_.close())

          override def closeWithMessage(message: T): Unit =
            endPoints foreach (_.closeWithMessage(message))
        }
    }
  }

  private def register[T](
      component: Component[T],
      topics: Seq[Topic[T]],
      jobId: Option[String] = None,
      partition: Option[Int] = None
  ): Unit =
    topics foreach (topic => register(component, topic, jobId, partition))

  private def register[T](
      component: Component[T],
      topic: Topic[T],
      jobId: Option[String] = None,
      partition: Option[Int] = None
  ): Unit = {
    val connector = connectors.getOrElse(topic, defaultConnector)
    topic match {
      case _: Exclusive[T] =>
        connector.registerExclusiveListener[T](component, topic.address(jobId, None))
      case _: WorkPull[T]  =>
        connector.registerWorkPullListener[T](component, topic.address(jobId, None))
      case _: Broadcast[T] =>
        connector.registerBroadcastListener[T](component, topic.address(jobId, None), partition.get)
      case _: Sharding[T]  =>
        connector.registerExclusiveListener[T](component, topic.address(jobId, partition))
    }
  }
}
