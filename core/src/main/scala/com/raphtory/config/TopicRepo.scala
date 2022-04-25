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

class TopicRepo(defaultConnector: Connector, conf: Config) {

  // Methods to override:
  def spoutOutputConnector: Connector  = defaultConnector
  def graphUpdatesConnector: Connector = defaultConnector
  def graphSyncConnector: Connector    = defaultConnector
  def queriesConnector: Connector      = defaultConnector
  def endedQueriesConnector: Connector = defaultConnector
  def watermarkConnector: Connector    = defaultConnector
  def queryPrepConnector: Connector    = defaultConnector

  def queryTrackConnector: Connector     = defaultConnector
  def rechecksConnector: Connector       = defaultConnector
  def jobStatusConnector: Connector      = defaultConnector
  def vertexMessagesConnector: Connector = defaultConnector
  def jobOperationsConnector: Connector  = defaultConnector

  // Configuration
  val spoutAddress: String     = conf.getString("raphtory.spout.topic")
  val depId: String            = conf.getString("raphtory.deploy.id")
  val partitionServers: Int    = conf.getInt("raphtory.partitions.serverCount")
  val partitionsPerServer: Int = conf.getInt("raphtory.partitions.countPerServer")
  val numPartitions: Int       = partitionServers * partitionsPerServer

  // Global topics
  def spoutOutput[T]: WorkPullTopic[T] = WorkPullTopic[T](spoutOutputConnector, spoutAddress)

  def graphUpdates: ShardingTopic[GraphUpdate] =
    ShardingTopic[GraphUpdate](numPartitions, graphUpdatesConnector, s"graph-updates-$depId")

  def graphSync: ShardingTopic[GraphUpdateEffect] =
    ShardingTopic[GraphUpdateEffect](numPartitions, graphSyncConnector, s"graph-sync-$depId")

  def queries: ExclusiveTopic[Query] = ExclusiveTopic[Query](queriesConnector, s"queries-$depId")

  def endedQueries: ExclusiveTopic[EndQuery] =
    ExclusiveTopic[EndQuery](endedQueriesConnector, s"ended-queries-$depId")

  def watermark: ExclusiveTopic[WatermarkTime] =
    ExclusiveTopic[WatermarkTime](watermarkConnector, s"watermark-$depId")

  def queryPrep: BroadcastTopic[QueryManagement] =
    BroadcastTopic[QueryManagement](queryPrepConnector, s"query-prep-$depId")

  // Job wise topics
  def queryTrack(jobId: String) =
    ExclusiveTopic[QueryManagement](queryTrackConnector, s"query-track-$depId")
  def rechecks                  = ExclusiveTopic[QueryManagement](rechecksConnector, s"recheck-$depId")
  def jobStatus                 = ExclusiveTopic[QueryManagement](jobStatusConnector, "job-status-$depId")

  def vertexMessages =
    ShardingTopic[GenericVertexMessage](
            vertexMessagesConnector,
            s"vertex-msg-$depId",
            numPartitions
    )
  def jobOperations  = BroadcastTopic[QueryManagement](jobOperationsConnector, s"job-ops-$depId")

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

}
