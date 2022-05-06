package com.raphtory.config.telemetry

import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/**
  * {s}`QueryTelemetry`
  *  : Adds metrics for {s}`QueryHandler`, {s}`QueryManager` and {s}`QueryExecutor`  using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of vertices, messages received and sent by {s}`Query` handler, manager and executor
  *    Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object QueryTelemetry {

  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  def receivedMessageCount() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_received_messages")
      .help("Total received messages count in Query Handler")
      .labelNames("jobID", "deploymentID")
      .register

  def sentMessageCount(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_sent_messages")
      .help("Total sent messages count in Query Handler")
      .labelNames("jobID", "deploymentID")
      .register

  def globalWatermarkMin(): Gauge =
    Gauge.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("manager_min_watermark_timestamp")
      .help("Minimum watermark for Query Manager")
      .labelNames("deploymentID")
      .register

  def globalWatermarkMax(): Gauge =
    Gauge.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("manager_max_watermark_timestamp")
      .help("Maximum watermark for Query Manager")
      .labelNames("deploymentID")
      .register

  def totalGraphOperations(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_graph_operations")
      .help("Total graph operations by Query Handler")
      .labelNames("jobID", "deploymentID")
      .register

  def totalTableOperations(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_table_operations")
      .help("Total table operations by Query Handler")
      .labelNames("jobID", "deploymentID")
      .register

  def totalPerspectivesProcessed(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_perspectives_processed")
      .help("Total perspectives processed by Query Handler")
      .labelNames("jobID", "deploymentID")
      .register

  def totalQueriesSpawned(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.query"))
      .name("manager_queries_spawned")
      .help("Total queries spawned by Query Manager")
      .labelNames("deploymentID")
      .register

}
