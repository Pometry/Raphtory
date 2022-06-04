package com.raphtory.formats

import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.raphtory.api.table.Row
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.TimeInterval
import com.typesafe.config.Config

import java.io.StringWriter

case class JsonFormat() extends Format {
  override def defaultDelimiter: String = "\n"

  override def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor =
    new SinkExecutor {
      private val gson                   = new GsonBuilder().setPrettyPrinting().create()
      private val stringWriter           = new StringWriter()
      private val jsonWriter: JsonWriter = new JsonWriter(stringWriter)

      jsonWriter.setIndent("  ")
      jsonWriter.beginObject()
      jsonWriter.name("jobID").value(jobID)
      jsonWriter.name("partitionID").value(partitionID)
      jsonWriter.name("perspectives")
      jsonWriter.beginArray()
      flush(stringWriter, connector)

      override def setupPerspective(perspective: Perspective): Unit = {
        jsonWriter.beginObject()
        jsonWriter.name("timestamp").value(perspective.timestamp)
        perspective.window match {
          case Some(DiscreteInterval(interval)) => jsonWriter.name("window").value(interval)
          case Some(TimeInterval(interval))     => jsonWriter.name("window").value(interval.toString)
          case _                                => jsonWriter.name("window").nullValue()
        }
        jsonWriter.name("rows")
        jsonWriter.beginArray()
        flush(stringWriter, connector)
      }

      override protected def writeRow(row: Row): Unit = {
        gson.toJson(row.getValues(), classOf[Array[Any]], jsonWriter)
        flush(stringWriter, connector)
      }

      override def closePerspective(): Unit = {
        jsonWriter.endArray()
        jsonWriter.endObject()
        flush(stringWriter, connector)
      }

      override def close(): Unit = {
        jsonWriter.endArray()
        jsonWriter.endObject()
        flush(stringWriter, connector)
        connector.closeItem()
        connector.close()
      }

      private def flush(stringWriter: StringWriter, connector: SinkConnector): Unit = {
        connector.write(stringWriter.toString)
        stringWriter.getBuffer.setLength(0)
      }
    }
}
