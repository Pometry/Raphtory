package com.raphtory.internals.communication

import com.raphtory.internals.components.output.OutputMessages
import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.EndQuery
import com.raphtory.internals.components.querymanager.GraphManagement
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.IngestionBlockingCommand
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.Submission
import com.raphtory.internals.components.querymanager.VertexMessagesSync
import com.raphtory.internals.components.querymanager.VertexMessaging
import com.raphtory.internals.components.querymanager.WatermarkTime
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration._
import com.typesafe.config.Config

private[raphtory] class TopicRepository(
    defaultControlConnector: Connector,
    defaultDataConnector: Connector,
    conf: Config
) {

  // Methods to override:
  protected def graphUpdatesConnector: Connector = defaultDataConnector
  protected def graphSyncConnector: Connector    = defaultDataConnector
  protected def outputConnector: Connector       = defaultDataConnector

  protected def submissionsConnector: Connector        = defaultControlConnector
  protected def completedQueriesConnector: Connector   = defaultControlConnector
  protected def watermarkConnector: Connector          = defaultControlConnector
  protected def blockingIngestionConnector: Connector  = defaultControlConnector
  protected def queryPrepConnector: Connector          = defaultControlConnector
  protected def ingestSetupConnector: Connector        = defaultControlConnector
  protected def partitionSetupConnector: Connector     = defaultControlConnector
  protected def queryTrackConnector: Connector         = defaultControlConnector
  protected def rechecksConnector: Connector           = defaultControlConnector
  protected def jobStatusConnector: Connector          = defaultControlConnector
  protected def vertexMessagesConnector: Connector     = defaultControlConnector
  protected def vertexMessagesSyncConnector: Connector = defaultControlConnector
  def jobOperationsConnector: Connector                = defaultControlConnector // accessed within the queryHandler

  // Configuration
  private val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  private val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  final def output(graphID: String, jobId: String): ExclusiveTopic[OutputMessages] =
    ExclusiveTopic[OutputMessages](outputConnector, "output", s"$graphID-$jobId")

  final def submissions(graphID: String): ExclusiveTopic[Submission] =
    ExclusiveTopic[Submission](submissionsConnector, s"submissions", graphID)

  final def completedQueries(graphID: String): ExclusiveTopic[EndQuery] =
    ExclusiveTopic[EndQuery](completedQueriesConnector, "completed.queries", graphID)

  final def ingestSetup(graphID: String): ExclusiveTopic[IngestData] =
    ExclusiveTopic[IngestData](ingestSetupConnector, "ingest.setup", graphID)

  final def graphSetup: ExclusiveTopic[ClusterManagement] =
    ExclusiveTopic[ClusterManagement](ingestSetupConnector, "graph.setup")

  final def clusterComms: BroadcastTopic[ClusterManagement] =
    BroadcastTopic[ClusterManagement](
            partitionServers + 2, // the number of ingestors, partitions and query managers
            ingestSetupConnector,
            "cluster.comms"
    )

  final def partitionSetup(graphID: String): BroadcastTopic[GraphManagement] =
    BroadcastTopic[GraphManagement](numPartitions, partitionSetupConnector, "partition.setup", graphID)

  // graph wise topics
  final def graphUpdates(graphID: String): ShardingTopic[GraphAlteration] =
    ShardingTopic[GraphAlteration](numPartitions, graphUpdatesConnector, s"graph.updates", graphID)

  final def graphSync(graphID: String): ShardingTopic[GraphUpdateEffect] =
    ShardingTopic[GraphUpdateEffect](numPartitions, graphSyncConnector, s"graph.sync", graphID)

  final def watermark(graphID: String): ExclusiveTopic[WatermarkTime] =
    ExclusiveTopic[WatermarkTime](watermarkConnector, "watermark", graphID)

  final def blockingIngestion(graphID: String): ExclusiveTopic[IngestionBlockingCommand] =
    ExclusiveTopic[IngestionBlockingCommand](blockingIngestionConnector, "blocking.ingestion", graphID)

  // Job wise topics
  final def queryTrack(graphID: String, jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](queryTrackConnector, "query.track", s"$graphID-$jobId")
  // Removed graphID from queryTrack because it is not necessary and complicates using on the webServer as it is not set

  final def rechecks(graphID: String, jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](rechecksConnector, "rechecks", s"$graphID-$jobId")

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

object TopicRepository {

  def apply(connector: Connector, conf: Config) =
    new TopicRepository(connector, connector, conf)

  def apply(controlConnector: Connector, dataConnector: Connector, conf: Config) =
    new TopicRepository(controlConnector, dataConnector, conf)
}
