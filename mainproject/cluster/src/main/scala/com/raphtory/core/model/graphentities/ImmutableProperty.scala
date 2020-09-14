package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.EntityStorage

class ImmutableProperty(creationTime: Long, key: String, value: Any, storage: EntityStorage) extends Property(creationTime) {
  var earliestTime: Long                = creationTime
  def earlist(): Long                   = creationTime
  override def valueAt(time: Long): Any = if (time >= earliestTime) value else ""
  override def valuesAfter(time: Long): Array[Any] = if (time >= earliestTime) Array[Any](value) else Array[Any]()
  override def update(msgTime: Long, newValue: Any): Unit =
    if (msgTime <= earliestTime) earliestTime = msgTime
  override def currentValue(): Any = value
}
