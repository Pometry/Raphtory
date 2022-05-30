package com.raphtory.config.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/** Adds metrics for `QueryHandler`, `QueryManager` and `QueryExecutor`  using Prometheus Client
  * Exposes Counter stats for tracking number of vertices, messages received and sent by `Query` handler, manager and executor
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object QueryTelemetry {

  def receivedMessageCount: Counter =
    Counter.build
      .namespace("query")
      .name("handler_received_messages")
      .help("Total received messages count in Query Handler")
      .labelNames("raphtory_jobID", "raphtory_deploymentID")
      .register

  def sentMessageCount: Counter =
    Counter.build
      .namespace("query")
      .name("handler_sent_messages")
      .help("Total sent messages count in Query Handler")
      .labelNames("raphtory_jobID", "raphtory_deploymentID")
      .register

  def globalWatermarkMin: Gauge =
    Gauge.build
      .namespace("query")
      .name("manager_min_watermark_timestamp")
      .help("Minimum watermark for Query Manager")
      .labelNames("raphtory_deploymentID")
      .register

  def globalWatermarkMax: Gauge =
    Gauge.build
      .namespace("query")
      .name("manager_max_watermark_timestamp")
      .help("Maximum watermark for Query Manager")
      .labelNames("raphtory_deploymentID")
      .register

  def totalGraphOperations: Counter =
    Counter.build
      .namespace("query")
      .name("handler_graph_operations")
      .help("Total graph operations by Query Handler")
      .labelNames("raphtory_jobID", "raphtory_deploymentID")
      .register

  def totalTableOperations: Counter =
    Counter.build
      .namespace("query")
      .name("handler_table_operations")
      .help("Total table operations by Query Handler")
      .labelNames("raphtory_jobID", "raphtory_deploymentID")
      .register

  def totalPerspectivesProcessed: Counter =
    Counter.build
      .namespace("query")
      .name("handler_perspectives_processed")
      .help("Total perspectives processed by Query Handler")
      .labelNames("raphtory_jobID", "raphtory_deploymentID")
      .register

  def totalQueriesSpawned: Counter =
    Counter.build
      .namespace("query")
      .name("manager_queries_spawned")
      .help("Total queries spawned by Query Manager")
      .labelNames("raphtory_deploymentID")
      .register

}
