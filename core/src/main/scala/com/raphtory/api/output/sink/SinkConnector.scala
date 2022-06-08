package com.raphtory.api.output.sink

/** Base trait for sink connectors.
  *
  * A `SinkConnector` enables implementations of `Format` to write just one or a collection of textual items.
  *
  * An item is a piece of information meaningful by itself and isolated from the rest of entities
  * from the format point of view.
  * Entities can be separated from the others using a natural separation existing for the technology in use.
  * For instance, in a message system, each item might be sent in a separate message.
  * If there doesn't exist such kind of separation, some kind of delimiter might be used.
  * For instance, in a file output, a character delimiter as `"\n"` might be used.
  * If we write just one item into the output, there shouldn't be any delimiter symbols as they are not needed.
  *
  * Implementations of this trait are intended to be used by calling `write` one or several times to compose an item
  * and `closeItem` when the current item is done.
  * Some formats might want to output just one item if its output cannot be split.
  *
  * @see [[com.raphtory.api.output.format.Format]]
  *      [[Sink]]
  */
trait SinkConnector {

  /** Appends `value` to the current item
    * @param value the value to append
    */
  def write(value: String): Unit

  /** Completes the writing of the current item */
  def closeItem(): Unit

  /** Ensures that the output of this sink completed and free up all the resources used by it. */
  def close(): Unit
}
