package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary

/**
  * {s}`PartitionTelemetry`
  *  : Adds metrics for partitions, i.e. {s}`Reader`, {s}`BatchWriter` and {s}`StreamWriter` using Prometheus Client
  *
  *    Exposes Counter and Summary stats for tracking number of graph updates, watermarks created by reader, vertices and edges added and deleted by writers in Raphtory
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  */
object PartitionTelemetry {

  val conf = new ConfigHandler().getConfig

  def lastWaterMarkProcessed(partitionID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.reader"))
      .name("last_watermark_processed_partitionID_" + partitionID + "_seconds")
      .help("Last watermark processed by Partition Reader")
      .register()

  def queryExecutorMapCounter(partitionID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.reader"))
      .name("query_executor_jobs_partitionID_" + partitionID + "_total")
      .help("Total Query Executor jobs created by Partition Reader")
      .register()

  def batchWriterVertexAdditions(partitionID: Int): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_vertex_adds_partitionID_" + partitionID.toString + "_total")
      .help("Total vertex additions for Batch Writer")
      .register()

  def batchWriterEdgeAdditions(partitionID: Int): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_edge_adds_partitionID_" + partitionID.toString + "_total")
      .help("Total edge additions for Batch Writer")
      .register()

  def batchWriterRemoteEdgeAdditions(partitionID: Int): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("batch_remote_edge_adds_partitionID_" + partitionID.toString + "_total")
      .help("Total remote edge additions for Batch Writer")
      .register()

  def streamWriterGraphUpdates(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_graph_updates_partitionID_" + partitionID + "_total")
      .help("Total graph updates for Stream Writer")
      .register()

  def totalSyncedStreamWriterUpdates(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_synced_updates_partitionID_" + partitionID + "_total")
      .help("Total synced updates for Stream Writer")
      .register()

  def streamWriterRemoteGraphUpdates(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_remote_graph_updates_partitionID_" + partitionID + "_total")
      .help("Total remote graph updates for Stream Writer")
      .register()

  def streamWriterVertexAdditions(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_vertex_adds_partitionID_" + partitionID + "_total")
      .help("Total vertex additions for Stream Writer")
      .register()

  def streamWriterVertexDeletions(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_vertex_deletes_partitionID_" + partitionID + "_total")
      .help("Total vertex deletions for Stream Writer")
      .register()

  def streamWriterEdgeAdditions(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_edge_adds_partitionID_" + partitionID + "_total")
      .help("Total edge additions for Stream Writer")
      .register()

  def streamWriterEdgeDeletions(partitionID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("stream_edge_deletes_partitionID_" + partitionID + "_total")
      .help("Total edge deletions for Stream Writer")
      .register()

  def totalTimeForIngestion(partitionID: Int): Summary =
    Summary
      .build()
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name("ingestion_time_partitionID_" + partitionID.toString)
      .help("Total time for ingestion")
      .create()

}
