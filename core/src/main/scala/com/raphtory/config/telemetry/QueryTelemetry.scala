package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import com.raphtory.config.telemetry.StorageTelemetry.conf
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/** Adds metrics for `QueryHandler`, `QueryManager` and `QueryExecutor`  using Prometheus Client
  * Exposes Counter and Gauge stats for tracking number of vertices, messages received and sent by `Query` handler, manager and executor
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object QueryTelemetry {

  val conf = new ConfigHandler().getConfig

  def receivedMessageCount(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_received_messages_$ID")
      .help("Total received messages count in Query Handler")
      .register

  def sentMessageCount(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_sent_messages_$ID")
      .help("Total sent messages count in Query Handler")
      .register

  def globalWatermarkMin(deploymentID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"manager_min_watermark_deploymentID_${deploymentID}_timestamp")
      .help("Minimum watermark for Query Manager")
      .register

  def globalWatermarkMax(deploymentID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"manager_max_watermark_deploymentID_${deploymentID}_timestamp")
      .help("Maximum watermark for Query Manager")
      .register

  def totalGraphOperations(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_graph_operations_$ID")
      .help("Total graph operations by Query Handler")
      .register

  def totalTableOperations(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_table_operations_$ID")
      .help("Total table operations by Query Handler")
      .register

  def totalPerspectivesProcessed(ID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"handler_perspectives_processed_$ID")
      .help("Total perspectives processed by Query Handler")
      .register

  def totalQueriesSpawned(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.query"))
      .name(s"manager_queries_spawned_deploymentID_$deploymentID")
      .help("Total queries spawned by Query Manager")
      .register

}
