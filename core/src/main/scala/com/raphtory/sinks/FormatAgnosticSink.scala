package com.raphtory.sinks

import com.raphtory.algorithms.api.Sink
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.formats.Format
import com.typesafe.config.Config

abstract class FormatAgnosticSink[T](format: Format) extends Sink {
  protected def createConnector(jobID: String, partitionID: Int, config: Config): SinkConnector

  override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor =
    format.executor(createConnector(jobID, partitionID, config))
}
