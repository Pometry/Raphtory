package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/**
  * {s}`StorageTelemetry`
  *  : Adds metrics for Raphtory storage components including {s}`GraphLens`, Vertex messaging and queue using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of files processed, lines parsed, spout reschedules and processing errors
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  */
object StorageTelemetry {

  val conf = new ConfigHandler().getConfig

  def pojoLensGraphSize(partitionID: String): Gauge =
    Gauge.build
      .namespace(conf.getString("raphtory.prometheus.namespaces.storage"))
      .name(s"pojo_lens_graph_size_${partitionID}_total")
      .help("Total graph size for Graph Lens")
      .register

}
