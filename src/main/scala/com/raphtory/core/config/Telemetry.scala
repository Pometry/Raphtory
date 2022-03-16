package com.raphtory.core.config

import io.prometheus.client.Counter

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

}
