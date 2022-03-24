package com.raphtory.core.config.telemetry

import com.raphtory.core.config.ConfigHandler
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

  val readyCount: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("total_files_processed")
      .help("Total files processed by spout")
      .register

  val vertexCount: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_vertex_count")
      .help("Total vertex count for query handler")
      .register

  val receivedMessageCount: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_msgs_received")
      .help("Total messages received by Query Handler")
      .register

  val sentMessageCount: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_msgs_sent")
      .help("Total messages sent by Query Handler")
      .register

  val totalTableOperations: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_table_operations")
      .help("Total table operations by Query Handler")
      .register

  val totalGraphOperations: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_graph_operations")
      .help("Total graph operations by Query Handler")
      .register

  val totalPerspectivesProcessed: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.query_namespace"))
      .name("query_handler_perspective_processed")
      .help("Total perspectives processed by Query Handler")
      .register

}