package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.PropertyValue

abstract private[raphtory] class Property {
  def update(msgTime: Long, index: Long, newValue: Any): Unit
  def valueAt(time: Long): collection.Iterable[Any]
  def values: collection.Iterable[PropertyValue[Any]]
  def valueHistory(after: Long = Long.MinValue, before: Long = Long.MaxValue): collection.Iterable[PropertyValue[Any]]
  def currentValue: Any
  def creation: Long
}
