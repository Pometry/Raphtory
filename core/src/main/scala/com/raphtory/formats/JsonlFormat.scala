package com.raphtory.formats

import com.google.gson.GsonBuilder
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector

case class JsonlFormat() extends Format {

  override def defaultDelimiter: String = "\n"

  override def executor(connector: SinkConnector): SinkExecutor =
    new SinkExecutor {
      private val gson           = new GsonBuilder().create()
      override def setupPerspective(perspective: Perspective): Unit = {}

      override protected def writeRow(row: Row): Unit = {
        val value = s"${gson.toJson(row.getValues())}"
        connector.write(value)
      }

      override def closePerspective(): Unit = {}

      override def close(): Unit = connector.close()
    }
}
