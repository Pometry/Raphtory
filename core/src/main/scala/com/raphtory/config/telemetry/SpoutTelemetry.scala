package com.raphtory.config.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import com.raphtory.config.ConfigHandler

/**
  * {s}`SpoutTelemetry`
  *  : Adds metrics for {s}`Spout` using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of files processed, lines parsed, spout reschedules and processing errors
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  */
object SpoutTelemetry {

  val conf = new ConfigHandler().getConfig

  val totalFilesProcessed: Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_files_processed")
      .help("Total files processed by spout")
      .register

  val totalSpoutReschedules: Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_spout_reschedules")
      .help("Total spout reschedules")
      .register

  val totalLinesParsed: Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_lines_parsed")
      .help("Total lines parsed")
      .register

  val totalFileProcessingErrors: Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("total_file_errors")
      .help("Total file processing errors")
      .register

}
