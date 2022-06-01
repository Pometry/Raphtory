package com.raphtory.formats

import com.raphtory.algorithms.api.SinkExecutor
import com.raphtory.sinks.SinkConnector

sealed trait Format {
  type T
  def defaultDelimiter: T
  def executor(connector: SinkConnector[T]): SinkExecutor
}

trait BinaryFormat extends Format {
  final type T = Array[Byte]
}

trait TextFormat extends Format {
  final type T = String
}
