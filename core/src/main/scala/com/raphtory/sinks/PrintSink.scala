package com.raphtory.sinks

import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.SinkConnector
import com.raphtory.api.output.sink.StreamSinkConnector
import com.raphtory.formats.CsvFormat
import com.typesafe.config.Config

case class PrintSink(format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  override protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector =
    new StreamSinkConnector(itemDelimiter) {
      override def output(value: String): Unit = System.out.print(value)
      override def close(): Unit               = System.out.println()
    }
}
