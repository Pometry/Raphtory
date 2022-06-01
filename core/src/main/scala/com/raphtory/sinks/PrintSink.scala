package com.raphtory.sinks

import com.raphtory.formats.CsvFormat
import com.raphtory.formats.Format
import com.typesafe.config.Config

class PrintSink(format: Format = CsvFormat()) extends FormatAgnosticSink(format) {

  override protected def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: Array[Byte]
  ): SinkConnector =
    new SinkConnector {
      override def write(value: Array[Byte]): Unit = System.out.write(value)
      override def closeItem(): Unit               = System.out.write(itemDelimiter)
      override def close(): Unit = {}
    }
}
