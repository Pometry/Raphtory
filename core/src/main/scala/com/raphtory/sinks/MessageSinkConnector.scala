package com.raphtory.sinks

import java.io.StringWriter

/** Helper class to implement SinkConnector trait based on message systems */
abstract class MessageSinkConnector extends SinkConnector {
  def sendAsync(message: String): Unit

  override val writer = new StringWriter()

  final override def closeItem(): Unit = {
    sendAsync(writer.toString)
    writer.getBuffer.setLength(0)
  }
}
