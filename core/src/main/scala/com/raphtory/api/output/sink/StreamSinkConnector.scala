package com.raphtory.api.output.sink

/** Base class for sink connectors which write items as an output stream.
  *
  * Implementations of this trait just need to override the `output` method.
  *
  * @param itemDelimiter the string to use as a delimiter between items
  */
abstract class StreamSinkConnector(itemDelimiter: String) extends SinkConnector {

  private var justClosedItem = false

  /** Append the given `value` to the output stream of this `SinkConnector`
    * @param value the value to be appended to the stream
    */
  protected def output(value: String): Unit

  final override def write(value: String): Unit = {
    if (justClosedItem) {
      output(itemDelimiter)
      justClosedItem = false
    }
    output(value)
  }

  final override def closeItem(): Unit = justClosedItem = true
}
