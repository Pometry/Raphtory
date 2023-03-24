package com.raphtory.formats

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.api.time.Perspective
import com.raphtory.internals.management.PythonInterop.repr
import com.typesafe.config.Config

/** A `Format` that writes a `Table` in comma-separated value (CSV) format
  *
  * This format outputs one CSV line per row.
  * The first two values are the timestamp used to create the perspective corresponding to that row
  * and the size of the window applied over the perspective.
  * If no window was applied over the perspective, the window size is omitted.
  * The following values are the values composing the row.
  *
  * For a table with just one perspective created from timestamp 10 with a window size 5 and 3 rows
  * the output might look as follows:
  *
  * {{{
  * 10,5,id1,12
  * 10,5,id2,13
  * 10,5,id3,24
  * }}}
  */
case class CsvFormat(delimiter: String = ",", includeHeader: Boolean = true) extends Format {
  override def defaultDelimiter: String = "\n"
  override def defaultExtension: String = "csv"

  override def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor =
    new SinkExecutor {
      private var firstPerspective                = true
      private var currentPerspective: Perspective = _
      private var currentHeader: List[String]     = _
      private var currentLinePrefix: String       = _
      private val mapper                          = JsonMapper.builder().addModule(DefaultScalaModule).build()

      private def ensureQuoted(str: String): String = {
        val needs_quoting = str.exists(s => delimiter.contains(s) || s == '\n' || s == '\r' || s == '"')
        if (needs_quoting)
          "\"" + str.replace("\"", "\"\"") + "\""
        else
          str
      }

      private def csvValue(value: Any): String =
        value match {
          case v: String => ensureQuoted(v)
          case v         =>
            ensureQuoted(mapper.writeValueAsString(v))
        }

      override def setupPerspective(perspective: Perspective, header: List[String]): Unit = {
        currentPerspective = perspective

        // Set the prefix for all the lines in this perspective containing timestamp (and window)
        perspective.window match {
          case Some(w) => currentLinePrefix = s"${perspective.timestampAsString}$delimiter$w$delimiter"
          case None    => currentLinePrefix = s"${perspective.timestampAsString}$delimiter"
        }

        // Write the header if this is the first perspective and explode if the header changes
        if (firstPerspective) {
          currentHeader = header
          if (includeHeader) {
            val csvColumns = header.mkString(delimiter)
            perspective.window match {
              case Some(w) => connector.writeHeader("timestamp,window," + csvColumns)
              case None    => connector.writeHeader("timestamp," + csvColumns)
            }
          }
          firstPerspective = false
        }
        else if (header != currentHeader) {
          val msg =
            "The number of columns needs to be consistent between perspectives when using the CsvFormat. " +
              s"Was previously '$currentHeader' and now is '$header'" +
              "You can explicitly set them using graph.select()"
          throw new IllegalStateException(msg)
        }
      }

      override protected def writeRow(row: Row): Unit = {
        connector.write(currentLinePrefix)
        connector.write(row.columns.values.map(csvValue).mkString(delimiter))
        connector.closeItem()
      }

      override def closePerspective(): Unit = {}
      override def close(): Unit               = connector.close()
    }
}
