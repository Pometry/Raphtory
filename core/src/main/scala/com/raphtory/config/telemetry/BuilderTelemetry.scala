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
  *    Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object BuilderTelemetry {
  val conf = new ConfigHandler().getConfig

  def totalVertexAdds(deploymentID: String) = {
    val counter: Counter = Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"vertex_add")
      .help("Total vertices added by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register
    counter.labels(deploymentID)
  }

  def totalVertexDeletes(deploymentID: String) = {
    val counter: Counter = Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"vertex_delete")
      .help("Total vertices deleted by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register
    counter.labels(deploymentID)
  }

  def totalEdgeAdds(deploymentID: String) = {
    val counter: Counter = Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"edge_add")
      .help("Total edges added by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register
    counter.labels(deploymentID)
  }

  def totalEdgeDeletes(deploymentID: String) = {
    val counter: Counter = Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"edge_delete")
      .help("Total edges deleted by Graph Builder")
      .labelNames("raphtory_deploymentID")
      .register
    counter.labels(deploymentID)
  }

  def totalGraphBuilderUpdates(deploymentID: String) = {
    val counter: Counter = Counter.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.builder"))
      .name(s"update")
      .help("Total Graph Builder updates")
      .labelNames("raphtory_deploymentID")
      .register
    counter.labels(deploymentID)
  }

}
