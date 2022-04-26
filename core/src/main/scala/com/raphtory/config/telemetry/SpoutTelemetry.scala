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
      .namespace(conf.getString("raphtory.prometheus.namespaces.spout"))
      .name("file_processed_total")
      .help("Total files processed by spout")
      .register

  val totalSpoutReschedules: Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.spout"))
      .name("reschedule_total")
      .help("Total spout reschedules")
      .register

  val totalLinesParsed: Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.spout"))
      .name("file_line_parsed_total")
      .help("Total lines of file parsed")
      .register

  val totalFileProcessingErrors: Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.spout"))
      .name("file_processing_error_total")
      .help("Total file processing errors")
      .register

}
