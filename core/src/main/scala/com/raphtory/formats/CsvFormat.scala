package com.raphtory.formats

import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector

case class CsvFormat() extends Format[String] {

  var currentPerspective: Perspective = _

  override def open(connector: SinkConnector[String]): Unit = {}

  override def setupPerspective(
      connector: SinkConnector[String],
      perspective: Perspective
  ): Unit = currentPerspective = perspective

  override def writeRow(connector: SinkConnector[String], row: Row): Unit =
    currentPerspective.window match {
      case Some(w) =>
        connector.write(s"${currentPerspective.timestamp},$w,${row.getValues().mkString(",")}")
      case None    =>
        connector.write(s"${currentPerspective.timestamp},${row.getValues().mkString(",")}")
    }

  override def closePerspective(connector: SinkConnector[String]): Unit = {}

  override def close(connector: SinkConnector[String]): Unit = {}
}
