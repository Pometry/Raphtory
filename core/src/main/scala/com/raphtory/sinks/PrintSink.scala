package com.raphtory.sinks

import com.raphtory.formats.CsvFormat
import com.raphtory.formats.Format
import com.typesafe.config.Config

import java.io.OutputStreamWriter
import java.io.Writer

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
