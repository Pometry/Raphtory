package com.raphtory.formats

import com.raphtory.api.analysis.table.Row
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.raphtory.api.time.Perspective
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
      private var currentPerspective: Perspective = _
      private var firstRow                        = true

      override def setupPerspective(perspective: Perspective): Unit =
        currentPerspective = perspective

      override protected def writeRow(row: Any): Unit = {
        if (header && firstRow) {
          val userFields = row match {
            case row: Row           => (1 to row.getValues().length).mkString(delimiter)
            case row: Seq[Any]      => (1 to row.length).mkString(delimiter)
            case row: Map[Any, Any] => row.keys.mkString(delimiter)
            case row: Product       => row.productElementNames.mkString(delimiter)
            case _                  => "value"
          }
          val header     = currentPerspective.window match {
            case Some(window) => s"timestamp${delimiter}window$delimiter$userFields"
            case None         => s"timestamp$delimiter$userFields"
          }
          connector.writeHeader(header)
          firstRow = false
        }
        val rowValues = getRowString(row) // TODO: check if the number of values per row are consistent
        val line      = currentPerspective.window match {
          case Some(window) =>
            s"${currentPerspective.timestamp},$window,$rowValues"
          case None         =>
            s"${currentPerspective.timestamp},$rowValues" // TODO: add empty field in empty position here
        }
        connector.write(line)
        connector.closeItem()
      }

      override def closePerspective(): Unit = {}

      override def close(): Unit                                    = connector.close()

      private def getRowString(row: Any): String =
        row match {
          case row: Row           => row.getValues().mkString(delimiter)
          case row: Seq[Any]      => row.mkString(delimiter)
          case row: Map[Any, Any] => row.values.mkString(delimiter)
          case row: Product       => row.productIterator.mkString(delimiter)
          case row                => row.toString
        }
    }
}
