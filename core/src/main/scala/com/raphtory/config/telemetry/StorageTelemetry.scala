package com.raphtory.config.telemetry

import com.raphtory.config.ConfigHandler
import com.raphtory.deployment.Raphtory
import com.typesafe.config.Config
import io.prometheus.client.Counter

/** Adds metrics for Raphtory storage components including `GraphLens`, Vertex messaging and queue using Prometheus Client
  * Exposes Counter stats for tracking number of files processed, lines parsed, spout reschedules and processing errors
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
object StorageTelemetry {

  private val raphtoryConfig: Config = Raphtory.getDefaultConfig()

  def pojoLensGraphSize(): Counter =
    Counter.build
      .namespace(raphtoryConfig.getString("raphtory.prometheus.namespaces.storage"))
      .name("pojo_lens_graph_size")
      .help("Total graph size for Graph Lens")
      .labelNames("raphtory_jobID")
      .register

}
