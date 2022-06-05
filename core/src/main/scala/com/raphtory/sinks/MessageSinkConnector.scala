package com.raphtory.sinks

/** Helper class to implement SinkConnector trait based on message systems */
abstract class MessageSinkConnector extends SinkConnector {

  private val stringBuilder = new StringBuilder()

  def sendAsync(message: String): Unit

  final override def write(value: String): Unit = stringBuilder.append(value)

  final override def closeItem(): Unit = {
    sendAsync(stringBuilder.toString())
    stringBuilder.clear()
  }
}
