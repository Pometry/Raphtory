package com.raphtory.internals.communication

import com.raphtory.internals.components.querymanager.ClusterManagement
import com.raphtory.internals.components.querymanager.EndQuery
import com.raphtory.internals.components.querymanager.EstablishGraph
import com.raphtory.internals.components.querymanager.GraphManagement
import com.raphtory.internals.components.querymanager.IngestData
import com.raphtory.internals.components.querymanager.Query
import com.raphtory.internals.components.querymanager.QueryManagement
import com.raphtory.internals.components.querymanager.Submission
import com.raphtory.internals.components.querymanager.VertexMessagesSync
import com.raphtory.internals.components.querymanager.VertexMessaging
import com.raphtory.internals.components.querymanager.WatermarkTime
import com.raphtory.internals.graph.GraphAlteration
import com.raphtory.internals.graph.GraphAlteration._
import com.raphtory.sinks.OutputMessages
import com.typesafe.config.Config

private[raphtory] class TopicRepository(
    defaultControlConnector: Connector,
    defaultDataConnector: Connector,
    conf: Config
) {

  // Methods to override:
  protected def spoutConnector: Connector            = defaultDataConnector
  protected def graphUpdatesConnector: Connector     = defaultDataConnector
  protected def graphSyncConnector: Connector        = defaultDataConnector
  protected def outputConnector: Connector           = defaultDataConnector
  protected def submissionsConnector: Connector      = defaultControlConnector
  protected def completedQueriesConnector: Connector = defaultControlConnector
  protected def watermarkConnector: Connector        = defaultControlConnector
  protected def queryPrepConnector: Connector        = defaultControlConnector
  protected def ingestSetupConnector: Connector      = defaultControlConnector
  protected def partitionSetupConnector: Connector   = defaultControlConnector

  protected def queryTrackConnector: Connector         = defaultControlConnector
  protected def rechecksConnector: Connector           = defaultControlConnector
  protected def jobStatusConnector: Connector          = defaultControlConnector
  protected def vertexMessagesConnector: Connector     = defaultControlConnector
  protected def vertexMessagesSyncConnector: Connector = defaultControlConnector
  def jobOperationsConnector: Connector                = defaultControlConnector // accessed within the queryHandler

  // Configuration
  private val spoutAddress: String     = conf.getString("raphtory.spout.topic")
  private val graphID: String          = conf.getString("raphtory.graph.id")
  private val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  private val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  private val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  final def spout[T]: WorkPullTopic[(T, Long)] =
    WorkPullTopic[(T, Long)](spoutConnector, "spout", customAddress = spoutAddress)

  final def output: ExclusiveTopic[OutputMessages] = ExclusiveTopic[OutputMessages](outputConnector, "output", graphID)

  final def submissions: ExclusiveTopic[Submission] =
    ExclusiveTopic[Submission](submissionsConnector, s"submissions", graphID)

  final def completedQueries: ExclusiveTopic[EndQuery] =
    ExclusiveTopic[EndQuery](completedQueriesConnector, "completed.queries", graphID)

  final def ingestSetup: ExclusiveTopic[IngestData] =
    ExclusiveTopic[IngestData](ingestSetupConnector, "ingest.setup", graphID)

  final def graphSetup: ExclusiveTopic[ClusterManagement] =
    ExclusiveTopic[ClusterManagement](ingestSetupConnector, "graph.setup")

  final def clusterComms: ExclusiveTopic[ClusterManagement] =
    ExclusiveTopic[ClusterManagement](ingestSetupConnector, "cluster.comms")

  final def partitionSetup: BroadcastTopic[GraphManagement] =
    BroadcastTopic[GraphManagement](numPartitions, partitionSetupConnector, "partition.setup", graphID)

  // graph wise topics
  final def graphUpdates(graphID: String): ShardingTopic[GraphAlteration] =
    ShardingTopic[GraphAlteration](numPartitions, graphUpdatesConnector, s"graph.updates", s"$graphID")

  final def graphSync(graphID: String): ShardingTopic[GraphUpdateEffect] =
    ShardingTopic[GraphUpdateEffect](numPartitions, graphSyncConnector, s"graph.sync", s"$graphID")

  final def watermark: ExclusiveTopic[WatermarkTime] =
    ExclusiveTopic[WatermarkTime](watermarkConnector, "watermark", graphID)

  final def blockingIngestion: ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](watermarkConnector, "blocking.ingestion", graphID)

  // Job wise topics
  final def queryTrack(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](queryTrackConnector, "query.track", s"$graphID-$jobId")

  final def rechecks(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](rechecksConnector, "rechecks", s"$graphID-$jobId")

  final def jobStatus(jobId: String): ExclusiveTopic[QueryManagement] =
    ExclusiveTopic[QueryManagement](jobStatusConnector, "job.status", s"$graphID-$jobId")

  final def vertexMessages(jobId: String): ShardingTopic[VertexMessaging] =
    ShardingTopic[VertexMessaging](
            numPartitions,
            vertexMessagesConnector,
            "vertex.messages",
            s"$graphID-$jobId"
    )

  final def vertexMessagesSync(jobId: String): ShardingTopic[VertexMessagesSync] =
    ShardingTopic[VertexMessagesSync](
            numPartitions,
            vertexMessagesSyncConnector,
            "vertex.messages.sync",
            s"$graphID-$jobId"
    )

  final def jobOperations(jobId: String): BroadcastTopic[QueryManagement] =
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
