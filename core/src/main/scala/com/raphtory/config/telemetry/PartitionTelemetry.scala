package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import io.prometheus.client.Summary

import scala.collection.mutable

/**
  * `PartitionTelemetry`
  *  : Adds metrics for partitions, i.e. `Reader`, `BatchWriter` and `StreamWriter` using Prometheus Client
  *
  *    Exposes Counter and Summary stats for tracking number of graph updates, watermarks created by reader, vertices and edges added and deleted by writers in Raphtory
  *    Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object PartitionTelemetry {

  val conf = new ConfigHandler().getConfig

  def lastWaterMarkProcessed(ID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.reader"))
      .name(s"last_watermark_processed_$ID")
      .help("Last watermark processed by Partition Reader")
      .register()

  def queryExecutorMapCounter(ID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.reader"))
      .name(s"query_executor_jobs_${ID}_total")
      .help("Total query executors running in this partition")
      .register()

  def batchWriterVertexAdditions(partitionID: Int): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"batch_vertex_adds_partitionID_${partitionID.toString}")
      .help("Total vertex additions for Batch Writer")
      .register()

  def batchWriterEdgeAdditions(partitionID: Int): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"batch_edge_adds_partitionID_${partitionID.toString}")
      .help("Total edge additions for Batch Writer")
      .register()

  def batchWriterRemoteEdgeAdditions(partitionID: Int): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"batch_remote_edge_adds_partitionID_${partitionID.toString}")
      .help("Total remote edge additions for Batch Writer")
      .register()

  def streamWriterGraphUpdates(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"stream_graph_updates_$ID")
      .help("Total graph updates for Stream Writer")
      .register()

  def totalSyncedStreamWriterUpdates(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"stream_synced_updates_$ID")
      .help("Total synced updates for Stream Writer")
      .register()

  def streamWriterVertexAdditions(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"stream_vertex_adds_$ID")
      .help("Total vertex additions for Stream Writer")
      .register()

  def streamWriterVertexDeletions(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"stream_vertex_deletes_$ID")
      .help("Total vertex deletions for Stream Writer")
      .register()

  def streamWriterEdgeAdditions(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"stream_edge_adds_$ID")
      .help("Total edge additions for Stream Writer")
      .register()

  def streamWriterEdgeDeletions(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"stream_edge_deletes_$ID")
      .help("Total edge deletions for Stream Writer")
      .register()

  def timeForIngestion(partitionID: Int): Gauge =
    Gauge
      .build()
      .namespace(conf.getString("raphtory.prometheus.namespaces.writer"))
      .name(s"ingestion_time_partitionID_${partitionID.toString}")
      .help("Time for ingestion of partition")
      .create()

}
