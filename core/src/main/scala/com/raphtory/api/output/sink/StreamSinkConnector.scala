package com.raphtory.api.output.sink

abstract class StreamSinkConnector(itemDelimiter: String) extends SinkConnector {

  private var justClosedItem = false

  def output(value: String): Unit

  final override def write(value: String): Unit = {
    if (justClosedItem) {
      output(itemDelimiter)
      justClosedItem = false
    }
    output(value)
  }

  final override def closeItem(): Unit = justClosedItem = true
}
