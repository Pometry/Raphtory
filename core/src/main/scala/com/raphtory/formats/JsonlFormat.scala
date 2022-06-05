package com.raphtory.formats

import com.google.gson.GsonBuilder
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internal.graph.Perspective
import com.typesafe.config.Config

case class JsonlFormat() extends Format {
  override def defaultDelimiter: String = "\n"

  override def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor =
    new SinkExecutor {
      private val gson           = new GsonBuilder().create()
      override def setupPerspective(perspective: Perspective): Unit = {}

      override protected def writeRow(row: Row): Unit = {
        val value = s"${gson.toJson(row.getValues())}"
        connector.write(value)
        connector.closeItem()
      }

      override def closePerspective(): Unit = {}

      override def close(): Unit = connector.close()
    }
}
