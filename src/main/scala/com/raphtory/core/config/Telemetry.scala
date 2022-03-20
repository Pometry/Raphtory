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

  val totalWaterMarksCreated: Counter =
    Counter
      .build
      .namespace("reader")
      .name("total_watermarks_created")
      .help("Total watermarks created")
      .register
}
