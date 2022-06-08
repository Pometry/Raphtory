package com.raphtory.sinks

import com.raphtory.api.analysis.table.Table
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.StreamSinkConnector
import com.raphtory.formats.CsvFormat
import com.raphtory.internals.management.client.GraphDeployment
import com.typesafe.config.Config

import java.io.File
import java.io.FileWriter

/** A `Sink` that writes a `Table` into files using the given `format`.
  *
  * The sink creates one directory with the job id as name inside `filepath`
  * and one file for every partition on the server inside that directory.
  *
  * @param filePath the filepath to write the table into
  * @param format the format to be used by this sink (`CsvFormat` by default)
  *
  * @example
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.sinks.FileSink
  * import com.raphtory.components.spout.instance.ResourceSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(ResourceSpout("resource"), graphBuilder)
  * val testDir = "/tmp/raphtoryTest"
  * val sink = FileSink(testDir)
  *
  * graph.execute(EdgeList()).writeTo(sink)
  * }}}
  * @see [[Sink]]
  *      [[Format]]
  *      [[CsvFormat]]
  *      [[Table]]
  *      [[com.raphtory.Raphtory]]
  */
case class FileSink(filePath: String, format: Format = CsvFormat())
        extends FormatAgnosticSink(format) {

  override protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector =
    new StreamSinkConnector(itemDelimiter) {
      private val workDirectory = s"$filePath/$jobID"
      new File(workDirectory).mkdirs()
      private val fileWriter    = new FileWriter(s"$workDirectory/partition-$partitionID")

      override def output(value: String): Unit = fileWriter.write(value)
      override def close(): Unit               = fileWriter.close()
    }
}
