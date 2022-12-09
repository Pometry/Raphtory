package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter

object TelemetryReporter {
  val totalTuplesProcessed: Counter = IngestionTelemetry.totalTuplesProcessed
  val totalSourceErrors: Counter    = IngestionTelemetry.totalSourceErrors
  val vertexAddCounter: Counter     = IngestionTelemetry.totalVertexAdds
  val edgeAddCounter: Counter       = IngestionTelemetry.totalEdgeAdds

  val writerVertexAdditions: Counter = PartitionTelemetry.writerVertexAdditions
  val writerEdgeAdditions: Counter   = PartitionTelemetry.writerEdgeAdditions

  val totalSentMessageCount: Counter      = QueryTelemetry.sentMessageCount
  val totalPerspectivesProcessed: Counter = QueryTelemetry.totalPerspectivesProcessed
  val totalGraphOperations: Counter       = QueryTelemetry.totalGraphOperations
  val totalTableOperations: Counter       = QueryTelemetry.totalTableOperations
  val totalQueriesSpawned: Counter        = QueryTelemetry.totalQueriesSpawned
}
