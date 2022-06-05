package com.raphtory.formats

import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.api.table.Row
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector
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
