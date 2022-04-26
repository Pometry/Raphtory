package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/**
  * {s}`GraphBuilderTelemetry`
  *  : Adds metrics for {s}`GraphBuilder` using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of vertices and edges added and deleted, total partitions created
  *    by the graph builder
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  */
object BuilderTelemetry {

  val conf = new ConfigHandler().getConfig

  def totalVertexAdds(builderID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name("vertex_add_" + builderID + "_total")
      .help("Total vertices added by Graph Builder")
      .register

  def totalVertexDeletes(builderID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name("vertex_delete_" + builderID + "_total")
      .help("Total vertices deleted by Graph Builder")
      .register

  def totalEdgeAdds(builderID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name("edge_add_" + builderID + "_total")
      .help("Total edges added by Graph Builder")
      .register

  def totalEdgeDeletes(builderID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name("edge_delete_" + builderID + "_total")
      .help("Total edges deleted by Graph Builder")
      .register

  def totalGraphBuilderUpdates(builderID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name("update_" + builderID + "_total")
      .help("Total Graph Builder updates")
      .register

}
