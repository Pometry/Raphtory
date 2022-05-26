package com.raphtory.config.telemetry

import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import io.prometheus.client.{Counter, Gauge}

import scala.collection.mutable

/** Adds metrics for partitions, i.e. `Reader`, `BatchWriter` and `StreamWriter` using Prometheus Client
  * Exposes Counter and Summary stats for tracking number of graph updates, watermarks created by reader, vertices and edges added and deleted by writers in Raphtory
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object PartitionTelemetry {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  def lastWatermarkProcessed() =
    Gauge.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.reader"))
      .name("last_watermark_processed")
      .help("Last watermark processed by Partition Reader")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def queryExecutorCounter() =
    Gauge.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.reader"))
      .name("query_executor_jobs_total")
      .help("Total query executors running in this partition")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def batchWriterVertexAdditions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_vertex_adds")
      .help("Total vertex additions for Batch Writer")
      .labelNames("raphtory_partitionID")
      .register()

  def batchWriterEdgeAdditions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_edge_adds")
      .help("Total edge additions for Batch Writer")
      .labelNames("raphtory_partitionID")
      .register()

  def batchWriterEdgeDeletions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_edge_deletions")
      .help("Total edge deletions for Batch Writer")
      .labelNames("raphtory_partitionID")
      .register()

  def batchWriterRemoteEdgeAdditions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_remote_edge_adds")
      .help("Total remote edge additions for Batch Writer")
      .labelNames("raphtory_partitionID")
      .register()

  def batchWriterRemoteEdgeDeletions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_remote_edge_deletions")
      .help("Total remote edge deletions for Batch Writer")
      .labelNames("raphtory_partitionID")
      .register()

  def streamWriterGraphUpdates() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_graph_updates")
      .help("Total graph updates for Stream Writer")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def totalSyncedStreamWriterUpdates() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_synced_updates")
      .help("Total synced updates for Stream Writer")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def streamWriterVertexAdditions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_vertex_adds")
      .help("Total vertex additions for Stream Writer")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def streamWriterVertexDeletions(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_vertex_deletes")
      .help("Total vertex deletions for Stream Writer")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def streamWriterEdgeAdditions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_edge_adds")
      .help("Total edge additions for Stream Writer")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  def streamWriterEdgeDeletions() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_edge_deletes")
      .help("Total edge deletions for Stream Writer")
      .labelNames("raphtory_partitionID", "raphtory_deploymentID")
      .register()

  //TODO: implement
  def timeForIngestion() =
    Counter
      .build()
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.writer"))
      .name("ingestion_time")
      .help("Time for ingestion of partition")
      .labelNames("raphtory_partitionID")
      .create()

}
