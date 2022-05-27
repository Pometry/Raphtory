package com.raphtory.output

import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective
import com.typesafe.config.Config

abstract class AbstractCsvOutputWriter extends OutputWriter {
  override type OutputType = String
  var currentPerspective: Perspective = _

  final override def setupPerspective(perspective: Perspective): Unit =
    currentPerspective = perspective

  final override def closePerspective(): Unit = {}

  final override def threadSafeWriteRow(row: Row): Unit               =
    currentPerspective.window match {
      case Some(w) =>
        sink.writeEntity(s"${currentPerspective.timestamp},$w,${row.getValues().mkString(",")}")
      case None    =>
        sink.writeEntity(s"${currentPerspective.timestamp},${row.getValues().mkString(",")}")
    }

  final override def close(): Unit = sink.close()
}
