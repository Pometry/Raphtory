package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.FileSink
import com.typesafe.config.Config

/** Writes the rows of a `Table` to the file specified by `filePath` in CSV format.
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
  *
  * @see [[com.raphtory.algorithms.api.OutputFormat]]
  *      [[com.raphtory.algorithms.api.Table]]
  *      [[com.raphtory.client.GraphDeployment]]
  *      [[com.raphtory.deployment.Raphtory]]
  */
case class FileOutputFormat(filePath: String) extends OutputFormat {

  class FileOutputWriter(filePath: String, jobID: String, partitionID: Int)
          extends AbstractCsvOutputWriter {
    override protected def createSink() = new FileSink(filePath, jobID, partitionID)
  }

  override def outputWriter(jobId: String, partitionID: Int, config: Config): OutputWriter =
    new FileOutputWriter(filePath, jobId, partitionID)
}
