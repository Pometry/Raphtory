package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.TimePoint

abstract private[raphtory] class Property extends PropertyView {
  def update(msgTime: Long, index: Long, newValue: Any): Unit
}

abstract private[raphtory] class PropertyView {
  def valueAt(time: IndexedValue): collection.Iterable[Any]
  def values: collection.Iterable[PropertyValue[Any]]

  def valueHistory(
      after: IndexedValue = TimePoint.first,
      before: IndexedValue = TimePoint.last
  ): collection.Iterable[PropertyValue[Any]]
  def currentValue: PropertyValue[Any]
  def earliest: PropertyValue[Any]
  def viewBetween(after: IndexedValue, before: IndexedValue): PropertyView
}
