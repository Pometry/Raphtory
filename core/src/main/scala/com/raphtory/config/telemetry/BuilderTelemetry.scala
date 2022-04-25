package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.{Counter, Gauge}

/**
  * {s}`GraphBuilderTelemetry`
  *  : Adds metrics for {s}`GraphBuilder` using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of vertices and edges added and deleted, total partitions created
  *    by the graph builder
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  *
  *
  */
object BuilderTelemetry {

  val conf = new ConfigHandler().getConfig

  def totalVertexAdds(builderID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_vertex_adds_" + builderID)
      .help("Total vertices added by graph builder")
      .register

  def totalEdgeAdds(builderID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_edge_adds")
      .help("Total edges added by graph builder")
      .register

  def totalVertexDeletes(builderID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_vertex_deletes_" + builderID)
      .help("Total vertices deleted by graph builder")
      .register

  def totalEdgeDeletes(builderID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_edge_deletes_" + builderID)
      .help("Total edges added by graph builder")
      .register

  def totalGraphBuilderUpdates(builderID: String): Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_total_updates_" + builderID)
      .help("Total graph builder updates")
      .register

}