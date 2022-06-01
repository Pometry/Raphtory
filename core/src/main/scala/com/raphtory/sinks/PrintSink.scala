package com.raphtory.sinks

import com.raphtory.formats.CsvFormat
import com.raphtory.formats.Format
import com.typesafe.config.Config

class PrintSink(format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  override protected def textConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String
  ): SinkConnector[String] =
    new SinkConnector[String] {
      override def write(value: String): Unit = System.out.print(value)

      override def closeItem(): Unit = System.out.print(itemDelimiter)

      override def close(): Unit = {}
    }
}
