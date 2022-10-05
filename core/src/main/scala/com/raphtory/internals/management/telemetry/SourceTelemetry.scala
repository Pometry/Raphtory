package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter

/** Adds metrics for `GraphBuilder` using Prometheus Client
  * Exposes Counter stats for tracking number of vertices and edges added and deleted, total partitions created
  * by the graph builder
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
private[raphtory] object SourceTelemetry {

  def totalTuplesProcessed: Counter =
    Counter.build
      .namespace("source")
      .name("total_tuples_processed")
      .help("Total tuples that a source has ingested and processed")
      .labelNames("sourceID", "graphID")
      .register

  def totalSourceErrors: Counter =
    Counter.build
      .namespace("source")
      .name("total_source_errors")
      .help("Total source processing errors")
      .labelNames("sourceID", "graphID")
      .register

  def totalVertexAdds =
    Counter.build
      .namespace("source")
      .name("vertex_add")
      .help("Total vertices added by a Source")
      .labelNames("sourceID", "graphID")
      .register

  def totalVertexDeletes =
    Counter.build
      .namespace("source")
      .name("vertex_delete")
      .help("Total vertices deleted a Source")
      .labelNames("sourceID", "graphID")
      .register

  def totalEdgeAdds =
    Counter.build
      .namespace("source")
      .name("edge_add")
      .help("Total edges added by a Source")
      .labelNames("sourceID", "graphID")
      .register

  def totalEdgeDeletes =
    Counter.build
      .namespace("source")
      .name("edge_delete")
      .help("Total edges deleted by a Source")
      .labelNames("sourceID", "graphID")
      .register

}
