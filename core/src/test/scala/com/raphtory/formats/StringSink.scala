package com.raphtory.formats

import com.raphtory.api.output.format.Format
import com.raphtory.api.output.sink.FormatAgnosticSink
import com.raphtory.api.output.sink.SinkConnector
import com.typesafe.config.Config

import java.io.StringWriter

case class StringSink(format: Format) extends FormatAgnosticSink(format) {
  private val stringWriter = new StringWriter()

  def output: String = stringWriter.toString

  override def buildConnector(
      jobID: String,
      partitionID: Int,
      config: Config,
      itemDelimiter: String,
      fileExtension: String
  ): SinkConnector =
    new SinkConnector {
      override def allowsHeader: Boolean      = true
      override def write(value: String): Unit = stringWriter.write(value)
      override def closeItem(): Unit          = stringWriter.write(itemDelimiter)
      override def close(): Unit = {}
    }
}
