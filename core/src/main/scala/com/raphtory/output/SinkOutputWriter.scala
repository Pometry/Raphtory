package com.raphtory.output

import com.raphtory.algorithms.api.OutputWriter

trait SinkOutputWriter extends OutputWriter {
  type OutputType

  protected val sink: Sink[OutputType] = createSink()

  protected def createSink(): Sink[OutputType]
}
