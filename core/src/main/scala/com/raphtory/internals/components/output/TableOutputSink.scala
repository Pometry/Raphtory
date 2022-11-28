package com.raphtory.internals.components.output

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.api.time.Perspective
import com.raphtory.internals.components.querymanager.QueryManagement
import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

final private[raphtory] case class PerspectiveResult(perspective: Perspective, totalPartitions: Int, rows: Array[Row])
        extends QueryManagement

private[raphtory] case class TableOutputSink(graphID: String) extends Sink {

  override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor =
    new SinkExecutor { // We don't use the sink interface to return rows anymore, we do it directly
      override def setupPerspective(perspective: Perspective): Unit = {}
      override protected def writeRow(row: Row): Unit = {}
      override def closePerspective(): Unit = {}
      override def close(): Unit = {}
    }
}
