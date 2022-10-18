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

  val writerVertexAdditions: Counter = PartitionTelemetry.writerVertexAdditions
  val writerEdgeAdditions: Counter   = PartitionTelemetry.writerEdgeAdditions
  val writerEdgeDeletions: Counter   = PartitionTelemetry.writerEdgeDeletions
  val writerVertexDeletions: Counter = PartitionTelemetry.writerVertexDeletions

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
