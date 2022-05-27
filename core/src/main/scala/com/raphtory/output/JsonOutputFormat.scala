package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.FileSink
import com.typesafe.config.Config

/** Writes the rows of a `Table` to the file specified by `filePath` in JSON format.
  * @param filePath Filepath for writing Raphtory output.
  */
case class JsonOutputFormat(filePath: String) extends OutputFormat {

  class JsonOutputWriter(filePath: String, jobID: String, partitionID: Int)
          extends AbstractJsonOutputWriter(jobID, partitionID) {
    override protected def createSink() = new FileSink(filePath, jobID, partitionID)
  }

  override def outputWriter(jobID: String, partitionID: Int, config: Config): OutputWriter =
    new JsonOutputWriter(filePath, jobID, partitionID)
}
