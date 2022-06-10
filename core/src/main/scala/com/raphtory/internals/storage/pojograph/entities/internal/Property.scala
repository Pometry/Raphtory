package com.raphtory.internals.storage.pojograph.entities.internal

abstract private[raphtory] class Property {
  def update(msgTime: Long, newValue: Any): Unit
  def valueAt(time: Long): Option[Any]
  def values(): Array[(Long, Any)]
  def valueHistory(after: Long = Long.MinValue, before: Long = Long.MaxValue): Array[(Long, Any)]
  def currentValue(): Any
  def creation(): Long
}
