package com.raphtory.core.config

import io.prometheus.client.{Counter, Gauge, Summary}

object Telemetry {
  val batchWriterGraphUpdates: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_graph_updates")
      .help("Total graph updates for batch writer")
      .register

  val totalWaterMarksCreated: Counter =
    Counter
      .build
      .namespace("reader")
      .name("total_watermarks_created")
      .help("Total watermarks created")
      .register

  val lastWaterMarkProcessed: Gauge =
    Gauge
      .build
      .namespace("reader")
      .name("last_watermark_processed")
      .help("Last reader watermark")
      .register

  val queryExecutorMapCounter: Counter =
    Counter
      .build
      .namespace("reader")
      .name("total_query_executor_jobs")
      .help("Total query executor jobs created")
      .register

  val batchWriterVertexAdditions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_vertex_adds")
      .help("Total vertex additions for batch writer")
      .register

  val batchWriterEdgeAdditions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_edge_adds")
      .help("Total edge additions for batch writer")
      .register

  val batchWriterRemoteEdgeAdditions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_remote_edge_adds")
      .help("Total remote edge additions for batch writer")
      .register

  val batchWriterEdgeDeletions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_batch_writer_edge_deletes")
      .help("Total edge deletions for batch writer")
      .register

  val streamWriterGraphUpdates: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_graph_updates")
      .help("Total graph updates for stream writer")
      .register

  val streamWriterVertexAdditions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_vertex_adds")
      .help("Total vertex additions for stream writer")
      .register

  val streamWriterEdgeAdditions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_edge_adds")
      .help("Total edge additions for stream writer")
      .register

  val streamWriterEdgeDeletions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_edge_deletes")
      .help("Total edge deletions for stream writer")
      .register

  val streamWriterVertexDeletions: Counter =
    Counter
      .build
      .namespace("writer")
      .name("total_stream_writer_vertex_deletes")
      .help("Total vertex deletions for stream writer")
      .register

  val totalTimeForIngestion: Summary =
    Summary
      .build()
      .name("writer")
      .help("Total time for ingestion")
      .create();
}
