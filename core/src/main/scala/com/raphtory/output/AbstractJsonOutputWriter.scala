package com.raphtory.output

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.TimeInterval

import java.io.StringWriter

abstract class AbstractJsonOutputWriter(jobID: String, partitionID: Int) extends SinkOutputWriter {
  override type OutputType = String

  val stringWriter      = new StringWriter()
  val jsonWriter        = new JsonWriter(stringWriter)
  val gsonBuilder: Gson = new GsonBuilder().setPrettyPrinting().create()
  jsonWriter.setIndent("  ")

  var firstPerspective = true

  jsonWriter.beginObject()
  jsonWriter.name("jobID").value(jobID)
  jsonWriter.name("partitionID").value(partitionID)
  jsonWriter.name("perspectives")
  jsonWriter.beginArray()

  final override def setupPerspective(perspective: Perspective): Unit = {
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

  final override def closePerspective(): Unit = {
    jsonWriter.endArray()
    jsonWriter.endObject()
  }

  final override def writeRow(row: Row): Unit =
    gsonBuilder.toJson(row.getValues(), classOf[Array[Any]], jsonWriter)

  final override def close(): Unit = {
    jsonWriter.endArray()
    jsonWriter.endObject()
    sink.writeEntity(stringWriter.toString)
    sink.close()
  }
}
