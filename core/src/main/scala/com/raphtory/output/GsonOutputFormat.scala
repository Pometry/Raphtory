package com.raphtory.output

import com.raphtory.algorithms.api.OutputFormat
import com.raphtory.algorithms.api.OutputWriter
import com.raphtory.output.sinks.FileSink
import com.typesafe.config.Config

case class GsonOutputFormat(filePath: String) extends OutputFormat {

  override def outputWriter(jobID: String, partitionID: Int, config: Config): OutputWriter =
    new GsonOutputWriter(filePath, jobID, partitionID)
}

/** Writes output for Raphtory Job to Gson Format */
class GsonOutputWriter(filePath: String, jobID: String, partitionID: Int)
        extends AbstractGsonOutputWriter {
  override protected def createSink() = new FileSink(filePath, jobID, partitionID)
}

/** Writes output for Raphtory Job to Gson Format */
object GsonOutputFormat {
  def apply(filePath: String) = new GsonOutputFormat(filePath)
}
