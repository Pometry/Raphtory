package com.raphtory.formats

import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector

case class CsvFormat() extends TextFormat {
  override def defaultDelimiter: String = "\n"

  override def executor(connector: SinkConnector[String]): SinkExecutor =
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
