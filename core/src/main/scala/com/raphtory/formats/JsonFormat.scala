package com.raphtory.formats

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.internals.graph.Perspective
import com.raphtory.internals.time.DiscreteInterval
import com.raphtory.internals.time.TimeInterval
import com.typesafe.config.Config

import java.io.StringWriter

//sealed trait Level
//case object ROW    extends Level
//case object GLOBAL extends Level

case class JsonFormat(level: JsonFormat.Level) extends Format {
  override def defaultDelimiter: String = "\n"

  override def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor =
    level match {
      case JsonFormat.GLOBAL => globalLevelExecutor(connector, jobID, partitionID)
      case JsonFormat.ROW    => rowLevelExecutor(connector)
    }

  private def printPerspectiveProperties(jsonWriter: JsonWriter, perspective: Perspective): Unit = {
    jsonWriter.name("timestamp").value(perspective.timestamp)
    perspective.window match {
      case Some(DiscreteInterval(interval)) => jsonWriter.name("window").value(interval)
      case Some(TimeInterval(interval))     => jsonWriter.name("window").value(interval.toString)
      case _                                => jsonWriter.name("window").nullValue()
    }
  }

  private def printRowObject(jsonWriter: JsonWriter, gson: Gson, row: Row): Unit =
    gson.toJson(row.getValues(), classOf[Array[Any]], jsonWriter)

  private def flush(stringWriter: StringWriter, connector: SinkConnector): Unit = {
    connector.write(stringWriter.toString)
    stringWriter.getBuffer.setLength(0)
  }

  private def rowLevelExecutor(connector: SinkConnector): SinkExecutor =
    new SinkExecutor {
      private val gson                   = new GsonBuilder().setPrettyPrinting().create()
      private val stringWriter           = new StringWriter()
      private val jsonWriter: JsonWriter = new JsonWriter(stringWriter)

      private var currentPerspective: Perspective = _

      override def setupPerspective(perspective: Perspective): Unit =
        currentPerspective = perspective

      override protected def writeRow(row: Row): Unit = {
        jsonWriter.beginObject()
        printPerspectiveProperties(jsonWriter, currentPerspective)
        jsonWriter.name("row")
        printRowObject(jsonWriter, gson, row)
        jsonWriter.endObject()

        flush(stringWriter, connector)
        connector.closeItem()
      }

      override def closePerspective(): Unit = {}
      override def close(): Unit                                    = connector.close()
    }

  private def globalLevelExecutor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int
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
        printPerspectiveProperties(jsonWriter, perspective)
        jsonWriter.name("rows")
        jsonWriter.beginArray()
        flush(stringWriter, connector)
      }

      override protected def writeRow(row: Row): Unit = {
        printRowObject(jsonWriter, gson, row)
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
    }
}

object JsonFormat {

//  sealed trait Level
//  case object ROW    extends Level
//  case object GLOBAL extends Level
  object Level extends Enumeration {
    type Level = Value
    val ROW, GLOBAL = Value
  }
}
