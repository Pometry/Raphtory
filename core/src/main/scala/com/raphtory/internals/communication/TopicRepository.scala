package com.raphtory.internals.communication

import com.raphtory.internals.components.output.OutputMessages
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.VertexMessagesSync
import com.raphtory.internals.components.querymanager.VertexMessaging
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration._
import com.typesafe.config.Config

private[raphtory] class TopicRepository(
    defaultControlConnector: Connector,
    defaultIngestionConnector: Connector,
    defaultAnalysisConnector: Connector,
    conf: Config
) {

  // Methods to override:
  protected def graphUpdatesConnector: Connector = defaultIngestionConnector
  protected def graphSyncConnector: Connector    = defaultIngestionConnector
  protected def outputConnector: Connector       = defaultIngestionConnector

  protected def submissionsConnector: Connector       = defaultControlConnector
  protected def completedQueriesConnector: Connector  = defaultControlConnector
  protected def watermarkConnector: Connector         = defaultControlConnector
  protected def blockingIngestionConnector: Connector = defaultControlConnector
  protected def queryPrepConnector: Connector         = defaultControlConnector
  protected def ingestSetupConnector: Connector       = defaultControlConnector
  protected def queryTrackConnector: Connector        = defaultControlConnector
  protected def rechecksConnector: Connector          = defaultControlConnector
  protected def jobStatusConnector: Connector         = defaultControlConnector
  def jobOperationsConnector: Connector               = defaultControlConnector // accessed within the queryHandler

  protected def vertexMessagesConnector: Connector     = defaultAnalysisConnector
  protected def vertexMessagesSyncConnector: Connector = defaultAnalysisConnector

  // Configuration
  private val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  private val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  final def output(graphID: String, jobId: String): ExclusiveTopic[OutputMessages] =
    ExclusiveTopic[OutputMessages](outputConnector, "output", s"$graphID-$jobId")

  // graph wise topics
  final def graphUpdates(graphID: String): ShardingTopic[GraphAlteration] =
    ShardingTopic[GraphAlteration](numPartitions, graphUpdatesConnector, s"graph.updates", graphID)

  final def graphSync(graphID: String): ShardingTopic[GraphUpdateEffect] =
    ShardingTopic[GraphUpdateEffect](numPartitions, graphSyncConnector, s"graph.sync", graphID)

  // Job wise topics
  final def queryTrack(graphID: String, jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](queryTrackConnector, "query.track", s"$graphID-$jobId")
  // Removed graphID from queryTrack because it is not necessary and complicates using on the webServer as it is not set

  final def jobStatus(graphID: String, jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](jobStatusConnector, "job.status", s"$graphID-$jobId")

  final def vertexMessages(graphID: String, jobId: String): ShardingTopic[VertexMessaging] =
    ShardingTopic[VertexMessaging](
            numPartitions,
            vertexMessagesConnector,
            "vertex.messages",
            s"$graphID-$jobId"
    )

  final def vertexMessagesSync(graphID: String, jobId: String): ShardingTopic[VertexMessagesSync] =
    ShardingTopic[VertexMessagesSync](
            numPartitions,
            vertexMessagesSyncConnector,
            "vertex.messages.sync",
            s"$graphID-$jobId"
    )

  final def jobOperations(graphID: String, jobId: String): BroadcastTopic[QueryManagement] =
    BroadcastTopic[QueryManagement](
            numPartitions,
            jobOperationsConnector,
            s"job.operations",
            s"$graphID-$jobId"
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
      partitionId: Int
  ): CancelableListener = {
    val listeners = topics
      .map {
        case topic: ShardingTopic[T]  => topic.exclusiveTopicForPartition(partitionId)
        case topic: CanonicalTopic[T] => topic
      }
      .groupBy(_.connector)
      .map { case (connector, topics) => connector.register(id, messageHandler, topics) }
      .toSeq

    CancelableListener(listeners)
  }
}

object TopicRepository {

  def apply(connector: Connector, conf: Config) =
    new TopicRepository(connector, connector, connector, conf)

  def apply(
      controlConnector: Connector,
      ingestionConnector: Connector,
      analysisConnector: Connector,
      conf: Config
  ) =
    new TopicRepository(controlConnector, ingestionConnector, analysisConnector, conf)
}
