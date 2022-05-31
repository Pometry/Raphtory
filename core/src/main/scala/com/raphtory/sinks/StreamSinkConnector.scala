package com.raphtory.sinks

abstract class StreamSinkConnector(entityDelimiter: String) extends SinkConnector[String] {

  def writeValue(value: String): Unit

  private var streamIsEmpty = true

  final override def write(item: String): Unit =
    if (streamIsEmpty)
      write(entityDelimiter + item)
    else {
      write(item)
      streamIsEmpty = true
    }

  private var justClosedItem = false

  final override def append(partialItem: String): Unit = {
    if (justClosedItem) {
      write(entityDelimiter)
      justClosedItem = false
    }
    write(partialItem)
  }

  final override def closeItem(): Unit =
    justClosedItem = true
}
