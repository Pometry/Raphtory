package com.raphtory.config.telemetry

import io.prometheus.client.Counter
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config

/** Adds metrics for `Spout` using Prometheus Client
  * Exposes Counter stats for tracking number of files processed, lines parsed, spout reschedules and processing errors
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object SpoutTelemetry {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  val totalFilesProcessed: Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.spout"))
      .name("file_processed_total")
      .help("Total files processed by spout")
      .labelNames("raphtory_deploymentID")
      .register

  val totalSpoutReschedules: Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.spout"))
      .name("reschedule_total")
      .help("Total spout reschedules")
      .labelNames("raphtory_deploymentID")
      .register

  val totalLinesSent: Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.spout"))
      .name("file_line_sent_total")
      .help("Total lines of file sent")
      .labelNames("raphtory_deploymentID")
      .register

  val totalFileProcessingErrors: Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.spout"))
      .name("file_processing_error_total")
      .help("Total file processing errors")
      .labelNames("raphtory_deploymentID")
      .register

}
