package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

private[raphtory] object ComponentTelemetryHandler {

  val totalTuplesProcessed: Counter = SourceTelemetry.totalTuplesProcessed
  val totalSourceErrors: Counter    = SourceTelemetry.totalSourceErrors
  val vertexAddCounter: Counter     = SourceTelemetry.totalVertexAdds
  val vertexDeleteCounter: Counter  = SourceTelemetry.totalVertexDeletes
  val edgeAddCounter: Counter       = SourceTelemetry.totalEdgeAdds
  val edgeDeleteCounter: Counter    = SourceTelemetry.totalEdgeDeletes

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
