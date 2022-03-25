package com.raphtory.core.config.telemetry

import com.raphtory.core.config.ConfigHandler
import io.prometheus.client.{Counter, Gauge}

/**
  * {s}`StorageTelemetry`
  *  : Adds metrics for Raphtory storage components including {s}`GraphLens`, Vertex messaging and queue using Prometheus Client
  *
  *    Exposes Counter and Gauge stats for tracking number of files processed, lines parsed, spout reschedules and processing errors
  *    Statistics are made available on http://localhost:8899 on running tests and can be visualised using Grafana dashboards
  *
  *
  */
object StorageTelemetry {

  val conf = new ConfigHandler().getConfig

  val pojoLensGraphSize: Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.spout_namespace"))
      .name("pojo_lens_graph_size")
      .help("Graph size for Graph Lens")
      .register

  val pojoLensVertexCount: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.storage_namespace"))
      .name("pojo_lens_vertex_count")
      .help("Vertex Count for Graph Lens")
      .register

  val graphLensReceivedMessageCount: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.storage_namespace"))
      .name("pojo_lens_received_messages")
      .help("Received Messages count for Graph Lens ")
      .register

  val totalVertexMessagesSent: Counter =
    Counter
      .build
      .namespace(conf.getString("raphtory.prometheus.storage_namespace"))
      .name("total_vertex_messages_sent")
      .help("Vertex messages sent")
      .register

  val batchSizeVertexMessages: Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.storage_namespace"))
      .name("batch_size_vertex_messages")
      .help("Batch size for vertex messages")
      .register


  val storageCountsPerServer: Gauge =
    Gauge
      .build
      .namespace(conf.getString("raphtory.prometheus.storage_namespace"))
      .name("partition_counts_per_server")
      .help("Partitions per server for storage")
      .register
}