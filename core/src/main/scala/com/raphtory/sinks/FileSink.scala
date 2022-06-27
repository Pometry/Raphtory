package com.raphtory.sinks

import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.formats.CsvFormat
import com.typesafe.config.Config

import java.io.File
import java.io.FileWriter

/** A [[com.raphtory.api.output.sink.Sink Sink]] that writes a `Table` into files using the given `format`.
  *
  * This sink creates a directory named after the jobID inside the provided `filepath`. Each partition on the server then writes into its own file within this directory.
  *
  * @param filePath the filepath to write the table into
  * @param format the format to be used by this sink (`CsvFormat` by default)
  *
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.FileSink
  * import com.raphtory.spouts.FileSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(FileSpout("/path/to/your/file"), graphBuilder)
  * val testDir = "/tmp/raphtoryTest"
  * val sink = FileSink(testDir)
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[com.raphtory.api.output.sink.Sink Sink]]
  *      [[com.raphtory.api.output.format.Format Format]]
  *      [[com.raphtory.formats.CsvFormat CsvFormat]]
  *      [[com.raphtory.api.analysis.table.Table Table]]
  *      [[com.raphtory.Raphtory Raphtory]]
  */
case class FileSink(filePath: String, format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  override def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String,
      fileExtension: String
  ): SinkConnector =
    new SinkConnector {
      private val workDirectory = s"$filePath/$jobID"
      new File(workDirectory).mkdirs()
      private val file          = s"$workDirectory/partition-$partitionID.$fileExtension"
      private val fileWriter    = new FileWriter(file)

      override def write(value: String): Unit = fileWriter.write(value)
      override def closeItem(): Unit          = fileWriter.write(itemDelimiter)
      override def close(): Unit              = fileWriter.close()
    }
}
