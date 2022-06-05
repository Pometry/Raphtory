package com.raphtory.sinks

import com.raphtory.api.analysis.table.Table
import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.Sink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.StreamSinkConnector
import com.raphtory.formats.CsvFormat
import com.raphtory.internal.management.client.GraphDeployment
import com.typesafe.config.Config

import java.io.File
import java.io.FileWriter

/** Writes the rows of a `Table` to the file specified by `filePath` in CSV format.
  *
  * @param filePath Filepath for writing Raphtory output.
  *
  * Usage:
  * (while querying or running algorithmic tests)
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.algorithms.api.OutputFormat
  * import com.raphtory.components.spout.instance.ResourceSpout
  *
  * val graphBuilder = new YourGraphBuilder()
  * val graph = Raphtory.stream(ResourceSpout("resource"), graphBuilder)
  * val testDir = "/tmp/raphtoryTest"
  * val outputFormat: OutputFormat = FileOutputFormat(testDir)
  *
  * graph.execute(EdgeList()).writeTo(outputFormat)
  * }}}
  * @see [[Sink]]
  *      [[Table]]
  *      [[GraphDeployment]]
  *      [[com.raphtory.deployment.Raphtory]]
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
