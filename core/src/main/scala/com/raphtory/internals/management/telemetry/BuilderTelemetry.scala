package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter

/** Adds metrics for `GraphBuilder` using Prometheus Client
  * Exposes Counter stats for tracking number of vertices and edges added and deleted, total partitions created
  * by the graph builder
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object BuilderTelemetry {

  def totalVertexAdds =
    Counter.build
      .namespace("graph_builder")
      .name("vertex_add")
      .help("Total vertices added by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalVertexDeletes =
    Counter.build
      .namespace("graph_builder")
      .name("vertex_delete")
      .help("Total vertices deleted by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalEdgeAdds =
    Counter.build
      .namespace("graph_builder")
      .name("edge_add")
      .help("Total edges added by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalEdgeDeletes =
    Counter.build
      .namespace("graph_builder")
      .name("edge_delete")
      .help("Total edges deleted by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalGraphBuilderUpdates =
    Counter.build
      .namespace("graph_builder")
      .name("update")
      .help("Total Graph Builder updates")
      .labelNames("raphtory_deploymentID")
      .register

}
