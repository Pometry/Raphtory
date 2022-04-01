package com.raphtory.config.telemetry

import io.prometheus.client.{Counter, Gauge, Summary}

/**
  * {s}`PartitionTelemetry`
  *  : Adds metrics for partitions, i.e. {s}`Reader`, {s}`BatchWriter` and {s}`StreamWriter`  using Prometheus Client
  *
  *    Exposes Counter and Summary stats for tracking number of graph updates, watermarks created by reader, vertices and edges added and deleted by writers in Raphtory
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  *
  *
  */
object PartitionTelemetry {

  def lastWaterMarkProcessed(partitionID: String): Gauge =
    Gauge
      .build
      .namespace("reader")
      .name("last_reader_watermark_" + partitionID.toString)
      .help("Last reader watermark").register()

  def queryExecutorMapCounter(partitionID: String): Gauge =
    Gauge
      .build
      .namespace("reader")
      .name("total_query_executor_jobs_" + partitionID.toString)
      .help("Total query executor jobs created").register()

  def batchWriterVertexAdditions(partitionID: Int): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_vertex_adds_" + partitionID.toString)
      .help("Total vertex additions for batch writer").register()

  def batchWriterEdgeAdditions(partitionID: Int): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_edge_adds_" + partitionID.toString)
      .help("Total edge additions for batch writer").register()

  def batchWriterRemoteEdgeAdditions(partitionID: Int): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_remote_edge_adds_" + partitionID.toString)
      .help("Total remote edge additions for batch writer").register()

  def streamWriterGraphUpdates(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_graph_updates_" + partitionID.toString)
      .help("Total graph updates for stream writer").register()

  def totalSyncedStreamWriterUpdates(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("synced_stream_writer_updates_" + partitionID.toString)
      .help("Total synced stream writer updates").register()

  def streamWriterRemoteGraphUpdates(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_remote_graph_updates_" + partitionID.toString)
      .help("Total remote graph updates for stream writer").register()

  def streamWriterVertexAdditions(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_vertex_adds" + partitionID.toString)
      .help("Total vertex additions for stream writer").register()

  def streamWriterEdgeAdditions(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_edge_adds" + partitionID.toString)
      .help("Total edge additions for stream writer").register()

  def streamWriterEdgeDeletions(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_edge_deletes" + partitionID.toString)
      .help("Total edge deletions for stream writer").register()

  def streamWriterVertexDeletions(partitionID: String): Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_vertex_deletes" + partitionID.toString)
      .help("Total vertex deletions for stream writer").register()

  def totalTimeForIngestion(partitionID: Int): Summary =
    Summary
      .build()
      .namespace("writer")
      .name("total_time_for_ingestion_" + partitionID.toString)
      .help("Total time for ingestion")
      .create();

}