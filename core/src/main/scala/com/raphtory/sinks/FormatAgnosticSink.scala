package com.raphtory.sinks

import com.raphtory.algorithms.api.Sink
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.formats.Format
import com.typesafe.config.Config

abstract class FormatAgnosticSink(format: Format) extends Sink {

  protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector

  final override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor =
    format.executor(buildConnector(jobID, partitionID, config, format.defaultDelimiter))
}
