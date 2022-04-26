package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/**
  * {s}`QueryTelemetry`
  *  : Adds metrics for {s}`QueryHandler`, {s}`QueryManager` and {s}`QueryExecutor`  using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of vertices, messages received and sent by {s}`Query` handler, manager and executor
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  */
object QueryTelemetry {

  val conf = new ConfigHandler().getConfig

  def readyCount(jobID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_ready_count_jobID_" + jobID + "_total")
      .help("Ready count for Query Handler")
      .register

  def globalWatermarkMin(jobID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("manager_min_watermark_jobID_" + jobID + "_timestamp_seconds")
      .help("Minimum watermark for Query Manager")
      .register

  def globalWatermarkMax(jobID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("manager_max_watermark_jobID_" + jobID + "_timestamp_seconds")
      .help("Maximum watermark for Query Manager")
      .register

  def totalGraphOperations(jobID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_graph_operations_jobID_" + jobID + "_total")
      .help("Total graph operations by Query Handler")
      .register

  def totalTableOperations(jobID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_table_operations_jobID_" + jobID + "_total")
      .help("Total table operations by Query Handler")
      .register

  def totalPerspectivesProcessed(jobID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("handler_perspectives_processed_jobID_" + jobID + "_total")
      .help("Total perspectives processed by Query Handler")
      .register

  def totalQueriesSpawned(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name("manager_queries_spawned_deploymentID_" + deploymentID + "_total")
      .help("Total queries spawned by Query Manager")
      .register

}
