package com.raphtory.sinks

import java.io.ByteArrayOutputStream

/** Helper class to implement SinkConnector trait based on message systems */
abstract class MessageSinkConnector extends SinkConnector {

  private val arrayStream = new ByteArrayOutputStream()

  def sendAsync(message: Array[Byte]): Unit
  def sendAsync(message: String): Unit

  final override def write(value: Array[Byte]): Unit = arrayStream.write(value)
  final override def write(value: String): Unit      = sendAsync(value)

  final override def closeItem(): Unit = {
    sendAsync(arrayStream.toByteArray)
    arrayStream.reset()
  }
}
