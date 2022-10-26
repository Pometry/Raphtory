package com.raphtory.internals.storage.arrow

sealed trait EntityId extends Any { self =>
  def id: Long
  def isLocal: Boolean

  def isGlobal: Boolean = !isLocal
}

/**
  * Returned when the global id is supposed to be persisted on this partition
  * and it has been found
  * @param id
  *  local id
  */
case class ExistsOnPartition(id: Long) extends AnyVal with EntityId {
  override def isLocal: Boolean = true
}

/**
  * Returned when the global id is NOT supposed to be persisted on this
  * partition
  * @param id
  * global id of the vertex
  */
case class GlobalId(id: Long) extends AnyVal with EntityId {
  override def isLocal: Boolean = false
}

/**
  * Returned when the global id is supposed to pe persisted on this partition
  * but it does not exist yet
  * @param globalId
  *  global id of the vertex
  */
case class NotFound(globalId: Long) extends AnyVal with EntityId {
  override def isLocal: Boolean = true

  override def id: Long = globalId
}
