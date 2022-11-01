package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

private[raphtory] object QueryTelemetry {

  def perspectiveGraphSize: Counter =
    Counter.build
      .namespace("partition")
      .name("graph_size")
      .help("Total vertices")
      .labelNames("jobID", "graphID", "timestamp", "window")
      .register

  def sentMessageCount: Counter =
    Counter.build
      .namespace("query")
      .name("handler_sent_messages")
      .help("Total sent messages count in Query Handler")
      .labelNames("jobID", "graphID")
      .register

  def totalGraphOperations: Counter =
    Counter.build
      .namespace("query")
      .name("handler_graph_operations")
      .help("Total graph operations by Query Handler")
      .labelNames("jobID", "graphID")
      .register

  def totalTableOperations: Counter =
    Counter.build
      .namespace("query")
      .name("handler_table_operations")
      .help("Total table operations by Query Handler")
      .labelNames("jobID", "graphID")
      .register

  def totalPerspectivesProcessed: Counter =
    Counter.build
      .namespace("query")
      .name("handler_perspectives_processed")
      .help("Total perspectives processed by Query Handler")
      .labelNames("jobID", "graphID")
      .register

  def totalQueriesSpawned: Counter =
    Counter.build
      .namespace("query")
      .name("manager_queries_spawned")
      .help("Total queries spawned by Query Manager")
      .labelNames("graphID")
      .register

}
