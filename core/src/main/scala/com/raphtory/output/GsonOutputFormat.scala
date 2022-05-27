package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.FileSink
import com.typesafe.config.Config

/** Writes the rows of a `Table` to the file specified by `filePath` in JSON format.
  * @param filePath Filepath for writing Raphtory output.
  */
case class GsonOutputFormat(filePath: String) extends OutputFormat {

  class GsonOutputWriter(filePath: String, jobID: String, partitionID: Int)
          extends AbstractGsonOutputWriter {
    override protected def createSink() = new FileSink(filePath, jobID, partitionID)
  }

  override def outputWriter(jobID: String, partitionID: Int, config: Config): OutputWriter =
    new GsonOutputWriter(filePath, jobID, partitionID)
}
