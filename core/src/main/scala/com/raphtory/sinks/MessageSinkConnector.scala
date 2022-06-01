package com.raphtory.sinks

import java.io.ByteArrayOutputStream

/** Helper class to implement SinkConnector trait based on message systems */
abstract class MessageBinarySinkConnector extends SinkConnector[Array[Byte]] {

  private val arrayStream = new ByteArrayOutputStream()

  def sendAsync(message: Array[Byte]): Unit

  final override def write(value: Array[Byte]): Unit = arrayStream.write(value)

  final override def closeItem(): Unit = {
    sendAsync(arrayStream.toByteArray)
    arrayStream.reset()
  }
}

abstract class MessageTextSinkConnector extends SinkConnector[String] {

  private val stringBuilder = new StringBuilder()

  def sendAsync(message: String): Unit

  final override def write(value: String): Unit = stringBuilder.append(value)

  final override def closeItem(): Unit = {
    sendAsync(stringBuilder.toString())
    stringBuilder.clear()
  }
}
