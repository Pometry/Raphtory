package com.raphtory.core.config

import io.prometheus.client.{Counter, Gauge}

object Telemetry {
  val totalFilesProcessed: Counter =
    Counter
      .build
      .namespace("spout")
      .name("total_files_processed")
      .help("Total files processed by spout")
      .register

  val totalSpoutReschedules: Counter =
    Counter
      .build
      .namespace("spout")
      .name("total_spout_reschedules")
      .help("Total spout reschedules")
      .register

  val totalLinesParsed: Gauge =
    Gauge
      .build
      .namespace("spout")
      .name("total_lines_parsed")
      .help("Total lines parsed")
      .register

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

}
