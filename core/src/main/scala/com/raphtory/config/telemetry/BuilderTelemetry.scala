package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.Counter

import scala.collection.mutable

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

  def totalVertexAdds(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"vertex_add_$deploymentID")
      .help("Total vertices added by Graph Builder")
      .register

  def totalVertexDeletes(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"vertex_delete_$deploymentID")
      .help("Total vertices deleted by Graph Builder")
      .register

  def totalEdgeAdds(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"edge_add_$deploymentID")
      .help("Total edges added by Graph Builder")
      .register

  def totalEdgeDeletes(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"edge_delete_$deploymentID")
      .help("Total edges deleted by Graph Builder")
      .register

  def totalGraphBuilderUpdates(deploymentID: String): Counter =
    Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"graph_builder_update_$deploymentID")
      .help("Total Graph Builder updates")
      .register

}
