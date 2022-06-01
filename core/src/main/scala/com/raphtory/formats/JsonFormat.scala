package com.raphtory.formats

import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.TimeInterval

case class JsonFormat(jobID: String, partitionID: Int) extends Format {

  override def executor(connector: SinkConnector): SinkExecutor =
    new SinkExecutor {
      private val gson                   = new GsonBuilder().setPrettyPrinting().create()
      private val jsonWriter: JsonWriter = new JsonWriter(connector.writer)

      jsonWriter.setIndent("  ")
      jsonWriter.beginObject()
      jsonWriter.name("jobID").value(jobID)
      jsonWriter.name("partitionID").value(partitionID)
      jsonWriter.name("perspectives")
      jsonWriter.beginArray()

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
      }

      override protected def writeRow(row: Row): Unit =
        gson.toJson(row.getValues(), classOf[Array[Any]], jsonWriter)

      override def closePerspective(): Unit = {
        jsonWriter.endArray()
        jsonWriter.endObject()
      }

      override def close(): Unit = {
        jsonWriter.endArray()
        jsonWriter.endObject()
        connector.close()
      }
    }
}
