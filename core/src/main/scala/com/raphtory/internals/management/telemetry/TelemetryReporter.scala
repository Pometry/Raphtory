package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter

object TelemetryReporter {
  val totalTuplesProcessed: Counter = IngestionTelemetry.totalTuplesProcessed
  val totalSourceErrors: Counter    = IngestionTelemetry.totalSourceErrors
  val vertexAddCounter: Counter     = IngestionTelemetry.totalVertexAdds
  val vertexDeleteCounter: Counter  = IngestionTelemetry.totalVertexDeletes
  val edgeAddCounter: Counter       = IngestionTelemetry.totalEdgeAdds
  val edgeDeleteCounter: Counter    = IngestionTelemetry.totalEdgeDeletes

  val writerVertexAdditions: Counter = PartitionTelemetry.writerVertexAdditions
  val writerEdgeAdditions: Counter   = PartitionTelemetry.writerEdgeAdditions
  val writerEdgeDeletions: Counter   = PartitionTelemetry.writerEdgeDeletions
  val writerVertexDeletions: Counter = PartitionTelemetry.writerVertexDeletions

  val totalSentMessageCount: Counter      = QueryTelemetry.sentMessageCount
  val totalPerspectivesProcessed: Counter = QueryTelemetry.totalPerspectivesProcessed
  val totalGraphOperations: Counter       = QueryTelemetry.totalGraphOperations
  val totalTableOperations: Counter       = QueryTelemetry.totalTableOperations
  val totalQueriesSpawned: Counter        = QueryTelemetry.totalQueriesSpawned
}
