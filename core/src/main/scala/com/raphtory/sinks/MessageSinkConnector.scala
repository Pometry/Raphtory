package com.raphtory.sinks

abstract class MessageSinkConnector extends SinkConnector[String] {
  def sendAsync(message: String): Unit

  private var currentPartialItem = ""

  final override def write(item: String): Unit = sendAsync(item)

  final override def append(partialItem: String): Unit = currentPartialItem += partialItem

  final override def closeItem(): Unit = {
    sendAsync(currentPartialItem)
    currentPartialItem = ""
  }
}
