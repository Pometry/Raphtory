package com.raphtory.api.output.format

import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.SinkExecutor
import com.typesafe.config.Config

trait Format {
  def defaultDelimiter: String

  def executor(
      connector: SinkConnector,
      jobID: String,
      partitionID: Int,
      config: Config
  ): SinkExecutor
}
