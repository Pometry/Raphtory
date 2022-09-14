package com.raphtory.internals.storage.arrow

sealed trait EntityId extends Any { self =>
  def id: Long
  def isLocal: Boolean

  def isGlobal: Boolean = !isLocal
}

case class LocalId(id: Long)  extends AnyVal with EntityId {
  override def isLocal: Boolean = true
}

case class GlobalId(id: Long) extends AnyVal with EntityId {
  override def isLocal: Boolean = false
}
