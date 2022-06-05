package com.raphtory.formats

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internal.graph.Perspective
import com.typesafe.config.Config

case class CsvFormat() extends Format {
  override def defaultDelimiter: String = "\n"

  override def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor =
    new SinkExecutor {
      var currentPerspective: Perspective = _

      override def setupPerspective(perspective: Perspective): Unit =
        currentPerspective = perspective

      override protected def writeRow(row: Row): Unit = {
        currentPerspective.window match {
          case Some(w) =>
            connector.write(s"${currentPerspective.timestamp},$w,${row.getValues().mkString(",")}")
          case None    =>
            connector.write(s"${currentPerspective.timestamp},${row.getValues().mkString(",")}")
        }
        connector.closeItem()
      }

      override def closePerspective(): Unit = {}

      override def close(): Unit                                    = connector.close()
    }
}
