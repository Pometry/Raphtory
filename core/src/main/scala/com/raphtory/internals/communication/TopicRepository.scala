package com.raphtory.internals.communication

import com.raphtory.internals.components.querymanager.EndQuery
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryLifeCycle
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.VertexMessagesSync
import com.raphtory.internals.components.querymanager.VertexMessaging
import com.raphtory.internals.components.querymanager.WatermarkTime
import com.raphtory.internals.graph.GraphAlteration._
import com.typesafe.config.Config

private[raphtory] class TopicRepository(
    defaultConnector: Connector,
    conf: Config,
    allConnectors: Array[Connector]
) {

  // Methods to override:
  protected def spoutConnector: Connector            = defaultConnector
  protected def graphUpdatesConnector: Connector     = defaultConnector
  protected def graphSyncConnector: Connector        = defaultConnector
  protected def submissionsConnector: Connector      = defaultConnector
  protected def completedQueriesConnector: Connector = defaultConnector
  protected def watermarkConnector: Connector        = defaultConnector
  protected def queryPrepConnector: Connector        = defaultConnector

  protected def queryTrackConnector: Connector         = defaultConnector
  protected def rechecksConnector: Connector           = defaultConnector
  protected def jobStatusConnector: Connector          = defaultConnector
  protected def vertexMessagesConnector: Connector     = defaultConnector
  protected def vertexMessagesSyncConnector: Connector = defaultConnector
  def jobOperationsConnector: Connector                = defaultConnector // accessed within the queryHandler

  // Configuration
  private val spoutAddress: String     = conf.getString("raphtory.spout.topic")
  private val depId: String            = conf.getString("raphtory.deploy.id")
  private val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  private val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  final def spout[T]: WorkPullTopic[T] =
    WorkPullTopic[T](spoutConnector, "spout", customAddress = spoutAddress)

  final def graphUpdates: ShardingTopic[GraphUpdate] =
    ShardingTopic[GraphUpdate](numPartitions, graphUpdatesConnector, s"graph.updates", depId)

  final def graphSync: ShardingTopic[GraphUpdateEffect] =
    ShardingTopic[GraphUpdateEffect](numPartitions, graphSyncConnector, s"graph.sync", depId)

  final def submissions: ExclusiveTopic[Query] =
    ExclusiveTopic[Query](submissionsConnector, s"submissions", depId)

  final def completedQueries: ExclusiveTopic[QueryLifeCycle] =
    ExclusiveTopic[QueryLifeCycle](completedQueriesConnector, "completed.queries", depId)

  final def watermark: ExclusiveTopic[WatermarkTime] =
    ExclusiveTopic[WatermarkTime](watermarkConnector, "watermark", depId)

  final def queryPrep: BroadcastTopic[QueryManagement] =
    BroadcastTopic[QueryManagement](numPartitions, queryPrepConnector, "query.prep", depId)

  // Job wise topics
  final def queryTrack(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](queryTrackConnector, "query.track", s"$depId-$jobId")

  final def rechecks(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](rechecksConnector, "rechecks", s"$depId-$jobId")

  final def jobStatus(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](jobStatusConnector, "job.status", s"$depId-$jobId")

  final def vertexMessages(jobId: String): ShardingTopic[VertexMessaging] =
    ShardingTopic[VertexMessaging](
            numPartitions,
            vertexMessagesConnector,
            "vertex.messages",
            s"$depId-$jobId"
    )

  final def vertexMessagesSync(jobId: String): ShardingTopic[VertexMessagesSync] =
    ShardingTopic[VertexMessagesSync](
            numPartitions,
            vertexMessagesSyncConnector,
            "vertex.messages.sync",
            s"$depId-$jobId"
    )

  final def jobOperations(jobId: String): BroadcastTopic[QueryManagement] =
    BroadcastTopic[QueryManagement](
            numPartitions,
            jobOperationsConnector,
            s"job.operations",
            s"$depId-$jobId"
    )

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
