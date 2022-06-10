package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter

/** Adds metrics for Raphtory storage components including `GraphLens`, Vertex messaging and queue using Prometheus Client
  * Exposes Counter stats for tracking number of files processed, lines parsed, spout reschedules and processing errors
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
private[raphtory] object StorageTelemetry {

  def pojoLensGraphSize: Counter =
    Counter.build
      .namespace("storage")
      .name("pojo_lens_graph_size")
      .help("Total graph size for Graph Lens")
      .labelNames("raphtory_jobID")
      .register

}
