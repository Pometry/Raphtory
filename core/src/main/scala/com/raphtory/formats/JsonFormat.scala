package com.raphtory.formats

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.raphtory.algorithms.api.Row
import com.raphtory.graph.Perspective
import com.raphtory.sinks.SinkConnector
import com.raphtory.time.DiscreteInterval
import com.raphtory.time.TimeInterval

import java.io.StringWriter

case class JsonFormat(jobID: String, partitionID: Int) extends Format[String] {

  private var stringWriter: StringWriter = _
  private var jsonWriter: JsonWriter     = _
  private var gsonBuilder: Gson          = _
  private var firstPerspective: Boolean  = _

  override def open(connector: SinkConnector[String]): Unit = {
    stringWriter = new StringWriter()
    jsonWriter = new JsonWriter(stringWriter)
    gsonBuilder = new GsonBuilder().setPrettyPrinting().create()
    firstPerspective = true

    jsonWriter.setIndent("  ")
    jsonWriter.beginObject()
    jsonWriter.name("jobID").value(jobID)
    jsonWriter.name("partitionID").value(partitionID)
    jsonWriter.name("perspectives")
    jsonWriter.beginArray()
  }

  override def setupPerspective(
      connector: SinkConnector[String],
      perspective: Perspective
  ): Unit = {
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

  override def writeRow(connector: SinkConnector[String], row: Row): Unit =
    gsonBuilder.toJson(row.getValues(), classOf[Array[Any]], jsonWriter)

  override def closePerspective(connector: SinkConnector[String]): Unit = {
    jsonWriter.endArray()
    jsonWriter.endObject()
  }

  override def close(connector: SinkConnector[String]): Unit = {
    jsonWriter.endArray()
    jsonWriter.endObject()
    connector.write(stringWriter.toString)
  }
}
