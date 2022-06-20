package com.raphtory.api.output.sink

/** Base trait for sink connectors.
  *
  * A `SinkConnector` enables implementations of `Format` to write just one or a collection of textual items.
  *
  * An item is a piece of information meaningful by itself and isolated from all other other entities from the perspective of the format.
  * Entities can be separated automatically from the others if such a concept exists in the underlying sinks technology.
  * For instance, in a message system such as Pulsar, each item might be sent in a separate message.
  * If this doesn't exist, a delimiter may instead be used.
  * For instance, in a file sink, a newline character delimiter `"\n"` can be used.
  *
  * Implementations of this trait are intended to be used by calling `write` one or more times to compose an item
  * and `closeItem` when the current item is completed.
  * Some formats might want to output just one item if its output cannot be split.
  *
  * @see [[com.raphtory.api.output.format.Format Format]]
  *      [[com.raphtory.api.output.sink.Sink Sink]]
  */
trait SinkConnector {

  /** Appends a `value` to the current item
    * @param value the value to append
    */
  def write(value: String): Unit

  /** Completes the writing of the current item */
  def closeItem(): Unit

  /** Ensures that the output of this sink completed and frees up all the resources used by it. */
  def close(): Unit
}
