package com.raphtory.config.telemetry


private[raphtory] object ComponentTelemetryHandler {
  val filesProcessed                          = SpoutTelemetry.totalFilesProcessed
  val spoutReschedules                        = SpoutTelemetry.totalSpoutReschedules
  val fileLinesSent                           = SpoutTelemetry.totalLinesSent
  val fileProcessingErrors                    = SpoutTelemetry.totalFileProcessingErrors
  val vertexAddCounter                        = BuilderTelemetry.totalVertexAdds()
  val vertexDeleteCounter                     = BuilderTelemetry.totalVertexDeletes()
  val edgeAddCounter                          = BuilderTelemetry.totalEdgeAdds()
  val edgeDeleteCounter                       = BuilderTelemetry.totalEdgeDeletes()
  val graphBuilderUpdatesCounter              = BuilderTelemetry.totalGraphBuilderUpdates()
  val lastWatermarkProcessedCollector         = PartitionTelemetry.lastWatermarkProcessed()
  val queryExecutorCollector                  = PartitionTelemetry.queryExecutorCounter()
  val batchWriterVertexAdditionsCollector     = PartitionTelemetry.batchWriterVertexAdditions()
  val batchWriterEdgeAdditionsCollector       = PartitionTelemetry.batchWriterEdgeAdditions()
  val batchWriterRemoteEdgeAdditionsCollector = PartitionTelemetry.batchWriterRemoteEdgeAdditions()
  val batchWriterRemoteEdgeDeletionsCollector = PartitionTelemetry.batchWriterRemoteEdgeDeletions()
  val batchWriterEdgeDeletionsCollector       = PartitionTelemetry.batchWriterEdgeDeletions()
  val vertexAddCollector                      = PartitionTelemetry.streamWriterVertexAdditions()
  val streamWriterGraphUpdatesCollector       = PartitionTelemetry.streamWriterGraphUpdates()
  val streamWriterVertexDeletionsCollector    = PartitionTelemetry.streamWriterVertexDeletions()
  val streamWriterEdgeAdditionsCollector      = PartitionTelemetry.streamWriterEdgeAdditions()
  val streamWriterEdgeDeletionsCollector      = PartitionTelemetry.streamWriterEdgeDeletions()
  val totalSyncedStreamWriterUpdatesCollector = PartitionTelemetry.totalSyncedStreamWriterUpdates()
  val receivedMessageCountCollector           = QueryTelemetry.receivedMessageCount()
  val totalSentMessageCount                   = QueryTelemetry.sentMessageCount()
  val totalPerspectivesProcessed              = QueryTelemetry.totalPerspectivesProcessed()
  val totalGraphOperations                    = QueryTelemetry.totalGraphOperations()
  val totalTableOperations                    = QueryTelemetry.totalTableOperations()
  val globalWatermarkMin                      = QueryTelemetry.globalWatermarkMin()
  val globalWatermarkMax                      = QueryTelemetry.globalWatermarkMax()
  val totalQueriesSpawned                     = QueryTelemetry.totalQueriesSpawned()
  val graphSizeCollector                      = StorageTelemetry.pojoLensGraphSize()
}
