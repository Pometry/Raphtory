package com.raphtory.sinks

import java.io.PrintStream

class PrintSink extends StreamSinkConnector("\n") {

  private val out: PrintStream = System.out

  override def writeValue(value: String): Unit = out.print(value)

  override def close(): Unit = {}
}
