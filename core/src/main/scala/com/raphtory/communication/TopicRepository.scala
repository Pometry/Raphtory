package com.raphtory.communication

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

/** @DoNotDocument */
class TopicRepository(defaultConnector: Connector, conf: Config) {

  // Methods to override:
  protected def spoutOutputConnector: Connector  = defaultConnector
  protected def graphUpdatesConnector: Connector = defaultConnector
  protected def graphSyncConnector: Connector    = defaultConnector
  protected def submissionsConnector: Connector  = defaultConnector
  protected def endedQueriesConnector: Connector = defaultConnector
  protected def watermarkConnector: Connector    = defaultConnector
  protected def queryPrepConnector: Connector    = defaultConnector

  protected def queryTrackConnector: Connector     = defaultConnector
  protected def rechecksConnector: Connector       = defaultConnector
  protected def jobStatusConnector: Connector      = defaultConnector
  protected def vertexMessagesConnector: Connector = defaultConnector
  protected def jobOperationsConnector: Connector  = defaultConnector

  // Configuration
  private val spoutAddress: String     = conf.getString("raphtory.spout.topic")
  private val depId: String            = conf.getString("raphtory.deploy.id")
  private val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  private val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  final def spoutOutput[T]: WorkPullTopic[T] =
    WorkPullTopic[T](spoutOutputConnector, "spoutOutput", customAddress = spoutAddress)

  final def graphUpdates: ShardingTopic[GraphUpdate] =
    ShardingTopic[GraphUpdate](numPartitions, graphUpdatesConnector, s"graphUpdates", depId)

  final def graphSync: ShardingTopic[GraphUpdateEffect] =
    ShardingTopic[GraphUpdateEffect](numPartitions, graphSyncConnector, s"graphSync", depId)

  final def submissions: ExclusiveTopic[Query] =
    ExclusiveTopic[Query](submissionsConnector, s"submissions", depId)

  final def endedQueries: ExclusiveTopic[EndQuery] =
    ExclusiveTopic[EndQuery](endedQueriesConnector, "endedQueries", depId)

  final def watermark: ExclusiveTopic[WatermarkTime] =
    ExclusiveTopic[WatermarkTime](watermarkConnector, "watermark", depId)

  final def queryPrep: BroadcastTopic[QueryManagement] =
    BroadcastTopic[QueryManagement](numPartitions, queryPrepConnector, "queryPrep", depId)

  // Job wise topics
  final def queryTrack(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](queryTrackConnector, "queryTrack", s"$depId-$jobId")

  final def rechecks(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](rechecksConnector, "rechecks", s"$depId-$jobId")

  final def jobStatus(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](jobStatusConnector, "jobStatus", s"$depId-$jobId")

  final def vertexMessages(jobId: String): ShardingTopic[QueryManagement] =
    ShardingTopic[QueryManagement](
            numPartitions,
            vertexMessagesConnector,
            "vertexMessages",
            s"$depId-$jobId"
    )

  final def jobOperations(jobId: String): BroadcastTopic[QueryManagement] =
    BroadcastTopic[QueryManagement](
            numPartitions,
            jobOperationsConnector,
            s"jobOperations",
            s"$depId-$jobId"
    )

//  // Registering functions
//  def registerBuilderExecutor(component: BuilderExecutor[Any]): CancelableListener =
//    registerListener(component.handleMessage, Seq(spoutOutput))
//
//  def registerStreamWriter(component: StreamWriter, partition: Int): CancelableListener =
//    registerListener(component.handleMessage, Seq(graphUpdates, graphSync), partition)
//
//  def registerReader(component: Reader): CancelableListener =
//    registerListener(component.handleMessage, Seq(queryPrep))
//
//  def registerQueryManager(component: QueryManager): CancelableListener =
//    registerListener(component.handleMessage, Seq(queries, endedQueries, watermark))
//
//  def registerQueryProgressTracker(
//      component: QueryProgressTracker,
//      jobId: String
//  ): CancelableListener =
//    registerListener(component.handleMessage, Seq(queryTrack(jobId)))
//
//  def registerQueryHandler(component: QueryHandler, jobId: String): CancelableListener =
//    registerListener(component.handleMessage, Seq(rechecks(jobId), jobStatus(jobId)))
//
//  def registerQueryExecutor(
//      component: QueryExecutor,
//      jobId: String,
//      partition: Int
//  ): CancelableListener =
//    registerListener(
//            component.handleMessage,
//            Seq(vertexMessages(jobId), jobOperations(jobId)),
//            partition
//    )

  final def registerListener[T](
      id: String,
      messageHandler: T => Unit,
      topic: CanonicalTopic[T]
  ): CancelableListener = registerListener(id, messageHandler, Seq(topic))

  final def registerListener[T](
      id: String,
      messageHandler: T => Unit,
      topics: Seq[CanonicalTopic[T]]
  ): CancelableListener = registerListener(id, messageHandler, topics, 0)

  final def registerListener[T](
      id: String,
      messageHandler: T => Unit,
      topic: Topic[T],
      partition: Int
  ): CancelableListener = registerListener(id, messageHandler, Seq(topic), partition)

  /** The id provided here must be unique among different deployments */
  final def registerListener[T](
      id: String,
      messageHandler: T => Unit,
      topics: Seq[Topic[T]],
      partition: Int
  ): CancelableListener = {
    val listeners = topics
      .map {
        case topic: ShardingTopic[T]  => topic.exclusiveTopicForPartition(partition)
        case topic: CanonicalTopic[T] => topic
      }
      .groupBy(_.connector)
      .map { case (connector, topics) => connector.register(id, messageHandler, topics) }
      .toSeq

    CancelableListener(listeners)
  }
}
