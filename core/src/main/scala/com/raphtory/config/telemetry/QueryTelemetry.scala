package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import com.raphtory.config.telemetry.StorageTelemetry.conf
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

  def readyCount(ID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_ready_count_${ID}_total")
      .help("Ready count for Query Handler")
      .register

  def vertexCount(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_vertex_count_${ID}_total")
      .help("Total vertex count in Query Handler")
      .register

  def receivedMessageCount(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_received_messages_${ID}_total")
      .help("Total received messages count in Query Handler")
      .register

  def sentMessageCount(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_sent_messages_${ID}_total")
      .help("Total sent messages count in Query Handler")
      .register

  def globalWatermarkMin(deploymentID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"manager_min_watermark_deploymentID_${deploymentID}_timestamp_seconds")
      .help("Minimum watermark for Query Manager")
      .register

  def globalWatermarkMax(deploymentID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"manager_max_watermark_deploymentID_${deploymentID}_timestamp_seconds")
      .help("Maximum watermark for Query Manager")
      .register

  def totalGraphOperations(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_graph_operations_${ID}_total")
      .help("Total graph operations by Query Handler")
      .register

  def totalTableOperations(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_table_operations_${ID}_total")
      .help("Total table operations by Query Handler")
      .register

  def totalPerspectivesProcessed(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_perspectives_processed_${ID}_total")
      .help("Total perspectives processed by Query Handler")
      .register

  def totalQueriesSpawned(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"manager_queries_spawned_deploymentID_${deploymentID}_total")
      .help("Total queries spawned by Query Manager")
      .register

}
