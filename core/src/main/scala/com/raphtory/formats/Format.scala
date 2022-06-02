package com.raphtory.formats

import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.sinks.SinkConnector
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
