package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.FileSink
import com.typesafe.config.Config

case class JsonOutputFormat(filePath: String) extends OutputFormat {

  override def outputWriter(jobID: String, partitionID: Int, config: Config): OutputWriter =
    new JsonOutputWriter(filePath, jobID, partitionID)
}

/** Writes output for Raphtory Job to GJson Format */
class JsonOutputWriter(filePath: String, jobID: String, partitionID: Int)
        extends AbstractJsonOutputWriter(jobID, partitionID) {
  override protected def createSink() = new FileSink(filePath, jobID, partitionID)
}

/** Writes output for Raphtory Job to Gson Format */
object JsonOutputFormat {
  def apply(filePath: String) = new GsonOutputFormat(filePath)
}
