package com.raphtory.sinks

import com.raphtory.algorithms.api.Sink
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.formats.Format
import com.typesafe.config.Config

abstract class FormatAgnosticSink[T](format: Format) extends Sink {

  protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: Array[Byte]
  ): SinkConnector

  final override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor = {
    val connector = buildConnector(jobID, partitionID, config, format.defaultItemDelimiter)
    format.executor(connector)
  }
}
