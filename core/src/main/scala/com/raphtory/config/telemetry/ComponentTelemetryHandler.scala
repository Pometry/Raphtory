package com.raphtory.config.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

private[raphtory] object ComponentTelemetryHandler {
  val filesProcessed: Counter       = SpoutTelemetry.totalFilesProcessed
  val spoutReschedules: Counter     = SpoutTelemetry.totalSpoutReschedules
  val fileLinesSent: Counter        = SpoutTelemetry.totalLinesSent
  val fileProcessingErrors: Counter = SpoutTelemetry.totalFileProcessingErrors

  val vertexAddCounter: Counter           = BuilderTelemetry.totalVertexAdds
  val vertexDeleteCounter: Counter        = BuilderTelemetry.totalVertexDeletes
  val edgeAddCounter: Counter             = BuilderTelemetry.totalEdgeAdds
  val edgeDeleteCounter: Counter          = BuilderTelemetry.totalEdgeDeletes
  val graphBuilderUpdatesCounter: Counter = BuilderTelemetry.totalGraphBuilderUpdates

  val lastWatermarkProcessedCollector: Gauge       = PartitionTelemetry.lastWatermarkProcessed
  val queryExecutorCollector: Gauge                = PartitionTelemetry.queryExecutorCounter
  val batchWriterVertexAdditionsCollector: Counter = PartitionTelemetry.batchWriterVertexAdditions
  val batchWriterEdgeAdditionsCollector: Counter   = PartitionTelemetry.batchWriterEdgeAdditions
  val batchWriterEdgeDeletionsCollector: Counter   = PartitionTelemetry.batchWriterEdgeDeletions

  val batchWriterRemoteEdgeAdditionsCollector: Counter =
    PartitionTelemetry.batchWriterRemoteEdgeAdditions

  val batchWriterRemoteEdgeDeletionsCollector: Counter =
    PartitionTelemetry.batchWriterRemoteEdgeDeletions

  val vertexAddCollector: Counter                 = PartitionTelemetry.streamWriterVertexAdditions
  val streamWriterGraphUpdatesCollector: Counter  = PartitionTelemetry.streamWriterGraphUpdates
  val streamWriterEdgeAdditionsCollector: Counter = PartitionTelemetry.streamWriterEdgeAdditions
  val streamWriterEdgeDeletionsCollector: Counter = PartitionTelemetry.streamWriterEdgeDeletions

  val streamWriterVertexDeletionsCollector: Counter =
    PartitionTelemetry.streamWriterVertexDeletions

  val totalSyncedStreamWriterUpdatesCollector: Counter =
    PartitionTelemetry.totalSyncedStreamWriterUpdates

  val receivedMessageCountCollector: Counter = QueryTelemetry.receivedMessageCount
  val totalSentMessageCount: Counter         = QueryTelemetry.sentMessageCount
  val totalPerspectivesProcessed: Counter    = QueryTelemetry.totalPerspectivesProcessed
  val totalGraphOperations: Counter          = QueryTelemetry.totalGraphOperations
  val totalTableOperations: Counter          = QueryTelemetry.totalTableOperations
  val globalWatermarkMin: Gauge              = QueryTelemetry.globalWatermarkMin
  val globalWatermarkMax: Gauge              = QueryTelemetry.globalWatermarkMax
  val totalQueriesSpawned: Counter           = QueryTelemetry.totalQueriesSpawned
  val graphSizeCollector: Counter            = StorageTelemetry.pojoLensGraphSize
}
