package com.raphtory.formats

import com.fasterxml.jackson.databind.json.JsonMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.raphtory.api.analysis.table.KeyPair
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
case class CsvFormat(delimiter: String = ",", header: Boolean = false) extends Format {
  override def defaultDelimiter: String = "\n"
  override def defaultExtension: String = "csv"

  override def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor =
    new SinkExecutor {
      private var currentHeader: List[String]     = _
      private var currentPerspective: Perspective = _
      private var firstRow                        = true
      private val mapper                          = JsonMapper.builder().addModule(DefaultScalaModule).build()

      private def ensureQuoted(str: String): String =
        if (
                (str.startsWith("\"") && str.endsWith("\""))
                || (str.startsWith("'") && str.endsWith("'"))
                || !str.contains(delimiter)
        )
          str
        else
          "\"" + str + "\""

      private def csvValue(obj: KeyPair): String =
        obj.value match {
          case v: String => ensureQuoted(v)
          case v         =>
            ensureQuoted(mapper.writeValueAsString(v))
        }

      override def setupPerspective(perspective: Perspective, header: List[String]): Unit = {
        currentHeader = header
        currentPerspective = perspective
      }

      override protected def writeRow(row: Row): Unit = {
        if (firstRow) {
          connector.writeHeader(currentHeader.mkString(delimiter))
          firstRow = false
        }
        val value = currentPerspective.window match {
          case Some(w) =>
            s"${currentPerspective.timestampAsString}$delimiter$w$delimiter${row.values().map(csvValue).mkString(delimiter)}"
          case None    =>
            s"${currentPerspective.timestampAsString}$delimiter${row.values().map(csvValue).mkString(delimiter)}"
        }
        connector.write(value)
        connector.closeItem()
      }

      override def closePerspective(): Unit = {}

      override def close(): Unit                 = connector.close()
    }
}
