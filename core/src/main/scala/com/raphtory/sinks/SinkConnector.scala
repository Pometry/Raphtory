package com.raphtory.sinks

/** An abstract sink of entities to be written out
  *
  * An entity is a piece of information meaningful by itself and isolated from the rest of entities.
  * Entities can be separated from the others using a natural separation existing for the technology in use.
  * For instance, in a message system, each entity might be sent in a separate message.
  * If there doesn't exist such kind of separation, some kind of delimiter might be used.
  * For instance, in a file output, a character delimiter as a "\n" might be used.
  * If we write just one entity into the output, there shouldn't be any delimiter symbols as they are not needed.
  *
  * Implementations of this trait are intended to be used by sequential calls to either `writeEntity` or
  * `append`/`closeEntity`
  * The behavior expected from combining both styles is undefined.
  *
  * @tparam OutputType The type of output: e.g. `String`
  */
trait SinkConnector[T] {

  /** Outputs an `entity`.
    * Several calls to writeEntity in a row must output the entities properly separated between them.
    * @param entity The entity to be output.
    */
  def write(item: T): Unit

  /** Append the `partialEntity` to the current entity being output.
    * When the Sink is created, and append is called for the first time the partial entity is assumed to be appended to
    * an empty entity, conforming therefore the initial part of the first entity.
    * @note Intended to be used in combination with `closeEntity`.
    * @param partialEntity
    */
  def append(partialItem: T): Unit

  /** Closes the current entity
    * @note Intended to be used in combination with append.
    */
  def closeItem(): Unit

  /** Ensures that the output of this sink completed and free up all the resources used by it. */
  def close(): Unit
}
