package com.raphtory.internals.management.telemetry

import io.prometheus.client.Counter
import io.prometheus.client.Gauge

/** Adds metrics for partitions, i.e. `Reader`, `BatchWriter` and `StreamWriter` using Prometheus Client
  * Exposes Counter and Summary stats for tracking number of graph updates, watermarks created by reader, vertices and edges added and deleted by writers in Raphtory
  * Statistics are made available on http://localhost:9999 on running tests and can be visualised using Grafana dashboards
  */
private[raphtory] object PartitionTelemetry {

  def lastWatermarkProcessed: Gauge =
    Gauge.build
      .namespace("partition")
      .name("last_watermark_processed")
      .help("Last safe watermark on this Partition")
      .labelNames("partitionID", "graphID")
      .register()

  def writerVertexAdditions: Counter =
    Counter.build
      .namespace("partition")
      .name("vertex_adds")
      .help("Total vertex additions")
      .labelNames("partitionID", "graphID")
      .register()

  def writerVertexDeletions: Counter =
    Counter.build
      .namespace("partition")
      .name("vertex_deletes")
      .help("Total vertex deletions")
      .labelNames("partitionID", "graphID")
      .register()

  def writerEdgeAdditions: Counter =
    Counter.build
      .namespace("partition")
      .name("edge_adds")
      .help("Total edge additions")
      .labelNames("partitionID", "graphID")
      .register()

  def writerEdgeDeletions: Counter =
    Counter.build
      .namespace("partition")
      .name("edge_deletes")
      .help("Total edge deletions")
      .labelNames("partitionID", "graphID")
      .register()

}
