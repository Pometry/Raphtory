package com.raphtory.config.telemetry

import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import io.prometheus.client.Counter

import scala.collection.mutable

/**
  * {s}`GraphBuilderTelemetry`
  *  : Adds metrics for {s}`GraphBuilder` using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of vertices and edges added and deleted, total partitions created
  *    by the graph builder
  *    Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object BuilderTelemetry {
  val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  def totalVertexAdds() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.builder"))
      .name("vertex_add")
      .help("Total vertices added by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalVertexDeletes() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.builder"))
      .name("vertex_delete")
      .help("Total vertices deleted by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalEdgeAdds() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.builder"))
      .name("edge_add")
      .help("Total edges added by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalEdgeDeletes() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.builder"))
      .name("edge_delete")
      .help("Total edges deleted by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register

  def totalGraphBuilderUpdates() =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.builder"))
      .name("update")
      .help("Total Graph Builder updates")
      .labelNames("raphtory_deploymentID")
      .register

}
