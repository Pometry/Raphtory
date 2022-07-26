package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.PropertyValue

import math.Ordering.Implicits._

private[raphtory] class ImmutableProperty(creationTime: Long, index: Long, value: Any) extends Property {
  private var propertyValue: PropertyValue[Any]   = PropertyValue(creationTime, index, value)
  override def valueAt(time: Long): Iterable[Any] = if (time >= propertyValue.time) Some(propertyValue.value) else None

  override def valueHistory(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Iterable[PropertyValue[Any]] =
    if (propertyValue.time > before)
      Array.empty[PropertyValue[Any]]
    else if (after > propertyValue.time)
      Array(propertyValue.copy(time = after, index = 0))
    else
      Array(propertyValue)

  override def update(msgTime: Long, index: Long, newValue: Any): Unit =
    propertyValue = propertyValue.min(PropertyValue(msgTime, index, newValue))

  override def currentValue: Any = propertyValue.value
  override def creation: Long    = propertyValue.time

  override def values: Iterable[PropertyValue[Any]] = Array(propertyValue)

}
