package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue

import scala.math.Ordering.Implicits.infixOrderingOps

private[raphtory] class ImmutableProperty(creationTime: Long, index: Long, value: Any) extends Property {
  private var propertyValue: PropertyValue[Any] = PropertyValue(creationTime, index, value)

  override def valueAt(time: IndexedValue): Iterable[Any] =
    if (time >= propertyValue) Some(propertyValue.value) else None

  override def valueHistory(
      after: IndexedValue,
      before: IndexedValue
  ): Iterable[PropertyValue[Any]] =
    if (propertyValue > before)
      Array.empty[PropertyValue[Any]]
    else if (after > propertyValue)
      Array(propertyValue.copy(time = after.time, index = after.index))
    else
      Array(propertyValue)

  override def update(msgTime: Long, index: Long, newValue: Any): Unit = {
    val updateValue = PropertyValue(msgTime, index, newValue)
    if (updateValue < propertyValue)
      propertyValue = updateValue
  }

  override def currentValue: PropertyValue[Any] = propertyValue
  override def earliest: PropertyValue[Any]     = propertyValue

  override def values: Iterable[PropertyValue[Any]] = Array(propertyValue)

  override def viewBetween(after: IndexedValue, before: IndexedValue): Property =
    if (after <= earliest) this
    else new ImmutableProperty(after.time, after.index, propertyValue.value)
}
