package com.raphtory.sinks

import com.raphtory.algorithms.api.Sink
import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.formats.BinaryFormat
import com.raphtory.formats.Format
import com.raphtory.formats.TextFormat
import com.typesafe.config.Config

abstract class FormatAgnosticSink(format: Format) extends Sink {

  protected def binaryConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: Array[Byte]
  ): SinkConnector[Array[Byte]] = null

  protected def textConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector[String] = null

  final override def executor(jobID: String, partitionID: Int, config: Config): SinkExecutor =
    format match {
      case format: BinaryFormat =>
        format.executor(binaryConnector(jobID, partitionID, config, format.defaultDelimiter))
      case format: TextFormat   =>
        format.executor(textConnector(jobID, partitionID, config, format.defaultDelimiter))
    }
}
