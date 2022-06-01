package com.raphtory.sinks

import java.io.PrintWriter
import java.io.Writer

class PrintSink extends SinkConnector {

  private val out = new PrintWriter(System.out)

  override def writer: Writer = new PrintWriter(System.out)

  override def closeItem(): Unit = out.write(itemDelimiter)

  override def close(): Unit = out.close()
}
