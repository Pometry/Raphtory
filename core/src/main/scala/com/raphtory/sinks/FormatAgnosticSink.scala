package com.raphtory.sinks

import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Sink
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.formats.Format
import com.raphtory.graph.Perspective
import com.typesafe.config.Config

abstract class FormatAgnosticSink[T](format: Format[T]) extends Sink {
  protected def createConnector(jobID: String, partitionID: Int, config: Config): SinkConnector[T]

  override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor =
    new SinkExecutor {
      val connector: SinkConnector[T] = createConnector(jobID, partitionID, config)
      format.open(connector)

      override def setupPerspective(perspective: Perspective): Unit =
        format.setupPerspective(connector, perspective)

      override protected def writeRow(row: Row): Unit = format.writeRow(connector, row)

      override def closePerspective(): Unit = format.closePerspective(connector)

      override def close(): Unit = {
        format.close(connector)
        connector.close()
      }
    }
}
