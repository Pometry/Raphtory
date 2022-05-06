package com.raphtory.config.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

private[raphtory] object ComponentTelemetryHandler {
  val vertexAddCounter                                 = BuilderTelemetry.totalVertexAdds()
  val vertexDeleteCounter                              = BuilderTelemetry.totalVertexDeletes()
  val edgeAddCounter                                   = BuilderTelemetry.totalEdgeAdds()
  val edgeDeleteCounter                                = BuilderTelemetry.totalEdgeDeletes()
  val lastWatermarkProcessedCollector: Gauge           = PartitionTelemetry.lastWatermarkProcessed()
  val queryExecutorCollector: Gauge                    = PartitionTelemetry.queryExecutorCounter()
  val graphSizeCollector: Gauge                        = StorageTelemetry.pojoLensGraphSize()
  val batchWriterVertexAdditionsCollector: Counter     = PartitionTelemetry.batchWriterVertexAdditions()
  val batchWriterEdgeAdditionsCollector: Counter       = PartitionTelemetry.batchWriterEdgeAdditions()

  val batchWriterRemoteEdgeAdditionsCollector: Counter =
    PartitionTelemetry.batchWriterRemoteEdgeAdditions()

  val batchWriterRemoteEdgeDeletionsCollector: Counter =
    PartitionTelemetry.batchWriterRemoteEdgeDeletions()
  val batchWriterEdgeDeletionsCollector: Counter       = PartitionTelemetry.batchWriterEdgeDeletions()
  val vertexAddCollector                               = PartitionTelemetry.streamWriterVertexAdditions()
  val streamWriterGraphUpdatesCollector                = PartitionTelemetry.streamWriterGraphUpdates()
  val streamWriterVertexDeletionsCollector             = PartitionTelemetry.streamWriterVertexDeletions()
  val streamWriterEdgeAdditionsCollector               = PartitionTelemetry.streamWriterEdgeAdditions()
  val streamWriterEdgeDeletionsCollector               = PartitionTelemetry.streamWriterEdgeDeletions()
  val totalSyncedStreamWriterUpdatesCollector          = PartitionTelemetry.totalSyncedStreamWriterUpdates()
  val receivedMessageCountCollector                    = QueryTelemetry.receivedMessageCount()
  val totalSentMessageCount                            = QueryTelemetry.sentMessageCount()
  val totalPerspectivesProcessed                       = QueryTelemetry.totalPerspectivesProcessed()
  val totalGraphOperations                             = QueryTelemetry.totalGraphOperations()
  val totalTableOperations                             = QueryTelemetry.totalTableOperations()
  val globalWatermarkMin                               = QueryTelemetry.globalWatermarkMin()
  val globalWatermarkMax                               = QueryTelemetry.globalWatermarkMax()
  val totalQueriesSpawned                              = QueryTelemetry.totalQueriesSpawned()
}
