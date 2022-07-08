package com.raphtory.api.output.sink

/** Base class for sink connectors based on message systems.
  *
  * Implementations of this trait just need to override the `sendAsync` method.
  */
abstract class MessageSinkConnector extends SinkConnector {

  private val stringBuilder = new StringBuilder()

  /** Sends the `message` asynchronously
    * @param message the message to be sent
    */
  protected def sendAsync(message: String): Unit

  final override def allowsHeader: Boolean = false

  final override def write(value: String): Unit = stringBuilder.append(value)

  final override def closeItem(): Unit = {
    sendAsync(stringBuilder.toString())
    stringBuilder.clear()
  }
}
