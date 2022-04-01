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

  var partitionID : Option[Int] = None

  val lastWaterMarkProcessed: Gauge.Builder =
    Gauge
      .build
      .namespace("reader")
      .help("Last reader watermark")

  val queryExecutorMapCounter: Gauge.Builder =
    Gauge
      .build
      .namespace("reader")
      .name("total_query_executor_jobs")
      .help("Total query executor jobs created")

  val batchWriterVertexAdditions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total vertex additions for batch writer")

  val batchWriterEdgeAdditions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total edge additions for batch writer")

  val batchWriterRemoteEdgeAdditions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total remote edge additions for batch writer")

  val streamWriterGraphUpdates: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total graph updates for stream writer")

  val totalSyncedStreamWriterUpdates: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total synced stream writer updates")

  val streamWriterRemoteGraphUpdates: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total remote graph updates for stream writer")

  val streamWriterVertexAdditions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total vertex additions for stream writer")

  val streamWriterEdgeAdditions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total edge additions for stream writer")

  val streamWriterEdgeDeletions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total edge deletions for stream writer")

  val streamWriterVertexDeletions: Counter.Builder =
    Counter
      .build
      .namespace("writer")
      .help("Total vertex deletions for stream writer")

  val totalTimeForIngestion: Summary =
    Summary
      .build()
      .name("writer")
      .help("Total time for ingestion")
      .create();

}