package com.raphtory.core.config.telemetry

import io.prometheus.client.{Counter, Gauge}

object SpoutTelemetry {
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

}