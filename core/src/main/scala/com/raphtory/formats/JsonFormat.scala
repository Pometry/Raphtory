package com.raphtory.formats

import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.stream.JsonWriter
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.api.time.DiscreteInterval
import com.raphtory.api.time.Perspective
import com.raphtory.api.time.TimeInterval
import com.typesafe.config.Config

import java.io.StringWriter

/** A `Format` that writes a `Table` in JSON format
  *
  * This format can be configured to work in two different ways, depending on the value for `level`:
  * - `JsonFormat.ROW` (the default option): creates one JSON object per row, following the format defined [here](https://jsonlines.org).
  * - `JsonFormat.GLOBAL`: creates a global JSON object for every partition of the table
  *
  * For a table with just one perspective created from timestamp 10 with no window and 3 rows
  * the output might look as follows if `level` is set to `JsonFormat.GLOBAL`:
  * {{{
  * {
  *   "jobID": "EdgeCount",
  *   "partitionID": 0,
  *   "perspectives": [
  *     {
  *       "timestamp": 10,
  *       "window": null,
  *       "rows": [
  *         [
  *           "id1",
  *           12
  *         ],
  *         [
  *           "id2",
  *           13
  *         ],
  *         [
  *           "id3",
  *           24
  *         ]
  *       ]
  *     }
  *   ]
  * }
  * }}}
  *
  * On the other hand, if `level` is not set or is set to `JsonFormat.ROW`, the output might look as follows:
  *
  * {{{
  * {"timestamp":10,"window":null,"row":["id1",12]}
  * {"timestamp":10,"window":null,"row":["id2",13]}
  * {"timestamp":10,"window":null,"row":["id3",24]}
  * }}}
  *
  * @param level the table level to create json objects
  */
case class JsonFormat(level: JsonFormat.Level = JsonFormat.ROW) extends Format {
  override def defaultDelimiter: String = "\n"
  override def defaultExtension: String = "json"

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
      private val gson         = new GsonBuilder().setPrettyPrinting().create()
      private val stringWriter = new StringWriter()

      private var currentPerspective: Perspective = _

      override def setupPerspective(perspective: Perspective): Unit =
        currentPerspective = perspective

      override protected def writeRow(row: Row): Unit = {
        val jsonWriter = new JsonWriter(stringWriter)
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

  /** The level to use for creating JSON objects */
  sealed trait Level

  /** JsonFormat level that creates one JSON object per row */
  case object ROW extends Level

  /** JsonFormat level that creates one global JSON object per partition */
  case object GLOBAL extends Level
}
