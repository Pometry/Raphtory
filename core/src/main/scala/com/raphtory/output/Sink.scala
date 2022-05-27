package com.raphtory.output

// A Sink is responsible fot outputting one or several entities of some type OutputType to the outside.
// An entity is a piece of information meaningful by itself and isolated from the rest of entities.
// Entities can be separated from the others using a natural separation existing for the technology in use.
// For instance, in a message system, each entity might be sent in a separate message
// If there doesn't exist such kind of separation, some kind of delimiter might be used.
// For instance, in a file output, a character delimiter as a "\n" might be used.
// If we write just one entity into the output, there should be any delimiter symbols as they are not needed
// The behavior expected from combining calls to append/closeEntity and writeEntity is undefined
trait Sink[OutputType] {
  // output an entity. Several calls to writeEntity in a row must output the entities properly separated between them.
  def writeEntity(entity: OutputType): Unit

  // intended to be used in combination with closeEntity. When the Sink is created, and append is called for the first time
  // the partial entity is assumed to be appended to an empty entity, conforming therefore the initial part of the first entity
  def append(partialEntity: OutputType): Unit

  // intended to be used in combination with append
  def closeEntity(): Unit

  // ensures that the output of this sink completed and free up all the resources used by it
  def close(): Unit
}
