package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.{Counter, Gauge}

/**
  * {s}`QueryTelemetry`
  *  : Adds metrics for {s}`QueryHandler`, {s}`QueryManager` and {s}`QueryExecutor`  using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of vertices, messages received and sent by {s}`Query` handler, manager and executor
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  *
  *
  */
object QueryTelemetry {

  val conf = new ConfigHandler().getConfig

  def readyCount(jobID: String): Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_ready_count_" + jobID)
      .help("Ready count for query handler ")
      .register

  def globalWatermarkMin(jobID: String): Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_manager_min_watermark_" + jobID)
      .help("Minimum watermark for QueryManager ")
      .register

  def globalWatermarkMax(jobID: String): Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_manager_max_watermark_" + jobID)
      .help("Maximum watermark for Query Manager ")
      .register

  def totalGraphOperations(jobID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_graph_operations_" + jobID)
      .help("Total graph operations by Query Handler")
      .register

  def totalTableOperations(jobID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_table_operations_" + jobID)
      .help("Total table operations by Query Handler")
      .register

  def totalPerspectivesProcessed(jobID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace_"))
      .name("query_handler_perspective_processed_" + jobID )
      .help("Total perspectives processed by Query Handler")
      .register

  def totalQueriesSpawned(jobID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_manager_total_query_count_" + jobID)
      .help("Total queries spawned by query manager")
      .register

  def newQueriesTracked(jobID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_manager_new_query_tracked_" + jobID)
      .help("New queries tracked by query manager")
      .register
}