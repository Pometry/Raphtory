package com.raphtory.sinks

import com.raphtory.formats.CsvFormat
import com.raphtory.formats.Format
import com.typesafe.config.Config

import java.io.BufferedOutputStream
import java.io.BufferedWriter
import java.io.File
import java.io.FileOutputStream
import java.io.FileWriter
import java.io.Writer

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
  * @see [[com.raphtory.algorithms.api.Sink]]
  *      [[com.raphtory.algorithms.api.Table]]
  *      [[com.raphtory.client.GraphDeployment]]
  *      [[com.raphtory.deployment.Raphtory]]
  */
case class FileSink(filePath: String, format: Format = CsvFormat())
        extends FormatAgnosticSink(format) {

  override protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: Array[Byte]
  ): SinkConnector =
    new SinkConnector {
      private val workDirectory        = s"$filePath/$jobID"
      new File(workDirectory).mkdirs()
      private val filename             = s"$workDirectory/partition-$partitionID"
      private val fileOutputStream     = new FileOutputStream(filename)
      private val bufferedOutputStream = new BufferedOutputStream(fileOutputStream)

      override def write(value: Array[Byte]): Unit = bufferedOutputStream.write(value)
      override def closeItem(): Unit               = bufferedOutputStream.write(itemDelimiter)
      override def close(): Unit                   = bufferedOutputStream.close()
    }
}
