package com.raphtory.formats

import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.sinks.SinkConnector

trait Format {
  def defaultDelimiter: String
  def executor(connector: SinkConnector): SinkExecutor
}
