package com.raphtory.api.output.sink

/** An abstract sink of entities to be written out
  *
  * An item is a piece of information meaningful by itself and isolated from the rest of entities.
  * Entities can be separated from the others using a natural separation existing for the technology in use.
  * For instance, in a message system, each item might be sent in a separate message.
  * If there doesn't exist such kind of separation, some kind of delimiter might be used.
  * For instance, in a file output, a character delimiter as a "\n" might be used.
  * If we write just one item into the output, there shouldn't be any delimiter symbols as they are not needed.
  *
  * Implementations of this trait are intended to be used by sequential calls to either `write` or
  * `append`/`closeItem`
  * The behavior expected from combining both styles is undefined.
  */
trait SinkConnector {

  def write(value: String): Unit

  /** Closes the current item
    * @note Intended to be used in combination with append.
    */
  def closeItem(): Unit

  /** Ensures that the output of this sink completed and free up all the resources used by it. */
  def close(): Unit
}
