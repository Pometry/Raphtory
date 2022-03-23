package com.raphtory.core.config.telemetry

import io.prometheus.client.{Counter, Gauge}
import com.raphtory.core.config.ConfigHandler

object SpoutTelemetry {

  val conf = new ConfigHandler().getConfig

  val totalFilesProcessed: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_files_processed")
      .help("Total files processed by spout")
      .register

  val totalSpoutReschedules: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_spout_reschedules")
      .help("Total spout reschedules")
      .register

  val totalLinesParsed: Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_lines_parsed")
      .help("Total lines parsed")
      .register

}