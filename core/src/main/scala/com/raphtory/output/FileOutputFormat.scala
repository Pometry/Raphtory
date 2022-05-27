package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.FileSink
import com.typesafe.config.Config

case class FileOutputFormat(filePath: String) extends OutputFormat {

  override def outputWriter(jobId: String, partitionID: Int, config: Config): OutputWriter =
    new FileOutputWriter(filePath, jobId, partitionID)
}

/** Writes output for Raphtory Job and Partition for a pre-defined window and timestamp to File
  * @param filePath Filepath for writing Raphtory output.
  *
  * Usage:
  * (while querying or running algorithmic tests)
  * {{{
  * import com.raphtory.algorithms.generic.EdgeList
  * import com.raphtory.output.FileOutputFormat
  * import com.raphtory.algorithms.api.OutputFormat
  * import com.raphtory.components.graphbuilder.GraphBuilder
  * import com.raphtory.components.spout.Spout
  *
  * val graph = Raphtory.createTypedGraph[T](Spout[T], GraphBuilder[T])
  * val testDir = "/tmp/raphtoryTest"
  * val outputFormat: OutputFormat = FileOutputFormat(testDir)
  *
  * graph.pointQuery(EdgeList(), outputFormat, 1595303181, List())
  * }}}
  *
  * @see [[com.raphtory.algorithms.api.OutputFormat]]
  *       [[com.raphtory.client.GraphDeployment]]
  *       [[com.raphtory.deployment.Raphtory]]
  */
class FileOutputWriter(filePath: String, jobID: String, partitionID: Int)
        extends AbstractCsvOutputWriter {
  override protected def createSink() = new FileSink(filePath, jobID, partitionID)
}

/** Writes output for Raphtory Job and Partition for a pre-defined window and timestamp to File */
object FileOutputFormat {

  /** @param filePath Filepath for writing Raphtory output. */
  def apply(filePath: String) = new FileOutputFormat(filePath)
}
