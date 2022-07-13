package com.raphtory.formats

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter
import com.fasterxml.jackson.databind.ObjectWriter
import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
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
  *   "jobID" : "EdgeCount",
  *   "partitionID" : 0,
  *   "perspectives" : [ {
  *     "timestamp" : 10,
  *     "window" : null,
  *     "rows" : [ [ "id1", 12 ], [ "id2", 13 ], [ "id3", 24 ] ]
  *   } ]
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

  private def printPerspectiveProperties(generator: JsonGenerator, perspective: Perspective): Unit = {
    generator.writeNumberField("timestamp", perspective.timestamp)
    generator.writeFieldName("window")
    perspective.window match {
      case Some(DiscreteInterval(interval)) => generator.writeNumber(interval)
      case Some(TimeInterval(interval))     => generator.writeString(interval.toString)
      case _                                => generator.writeNull()
    }
  }

  private def printRowObject(generator: JsonGenerator, serializer: ObjectWriter, row: Any): Unit = {
    val valueToPrint = row match {
      case row: Row => row.getValues()
      case _        => row
    }
    serializer.writeValue(generator, valueToPrint)
  }

  private def flush(generator: JsonGenerator, stringWriter: StringWriter, connector: SinkConnector): Unit = {
    generator.flush()
    connector.write(stringWriter.toString)
    stringWriter.getBuffer.setLength(0)
  }

  private def rowLevelExecutor(connector: SinkConnector): SinkExecutor =
    new SinkExecutor {
      private val stringWriter = new StringWriter()
      private val mapper       = JsonMapper.builder().addModule(DefaultScalaModule).build()
      private val serializer   = mapper.writer()

      private var currentPerspective: Perspective = _

      override def setupPerspective(perspective: Perspective): Unit =
        currentPerspective = perspective

      override protected def writeRow(row: Any): Unit = {
        val generator = mapper.createGenerator(stringWriter)
        generator.writeStartObject()
        printPerspectiveProperties(generator, currentPerspective)
        generator.writeFieldName("row")
        printRowObject(generator, serializer, row)
        generator.writeEndObject()

        flush(generator, stringWriter, connector)
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
      private val stringWriter = new StringWriter()
      private val mapper       = JsonMapper.builder().addModule(DefaultScalaModule).build()
      private val serializer   = mapper.writer()
      private val generator    = mapper.createGenerator(stringWriter)

      generator.setPrettyPrinter(new DefaultPrettyPrinter())

      generator.writeStartObject()
      generator.writeStringField("jobID", jobID)
      generator.writeNumberField("partitionID", partitionID)
      generator.writeArrayFieldStart("perspectives")
      flush(generator, stringWriter, connector)

      override def setupPerspective(perspective: Perspective): Unit = {
        generator.writeStartObject()
        printPerspectiveProperties(generator, perspective)
        generator.writeArrayFieldStart("rows")
        flush(generator, stringWriter, connector)
      }

      override protected def writeRow(row: Any): Unit = {
        printRowObject(generator, serializer, row)
        flush(generator, stringWriter, connector)
      }

      override def closePerspective(): Unit = {
        generator.writeEndArray()
        generator.writeEndObject()
        flush(generator, stringWriter, connector)
      }

      override def close(): Unit = {
        generator.writeEndArray()
        generator.writeEndObject()
        flush(generator, stringWriter, connector)
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
