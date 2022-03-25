package com.raphtory.core.config.telemetry

import com.raphtory.core.config.ConfigHandler
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
object GraphBuilderTelemetry {

  val conf = new ConfigHandler().getConfig

  val totalVertexAdds: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_vertex_adds")
      .help("Total vertices added by graph builder")
      .register

  val totalEdgeAdds: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_edge_adds")
      .help("Total edges added by graph builder")
      .register

  val totalVertexDeletes: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_vertex_deletes")
      .help("Total vertices deleted by graph builder")
      .register

  val totalEdgeDeletes: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_edge_deletes")
      .help("Total edges added by graph builder")
      .register

  val totalGraphBuilderUpdates: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_total_updates")
      .help("Total graph builder updates")
      .register

  val totalBuilderPartitions: Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.builder_namespace"))
      .name("graph_builder_partitions_count")
      .help("Total partitions created by graph builder")
      .register

}