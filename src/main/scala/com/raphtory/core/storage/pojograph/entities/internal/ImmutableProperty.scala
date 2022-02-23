package com.raphtory.core.storage.pojograph.entities.internal

class ImmutableProperty(creationTime: Long, value: Any) extends Property {
  var earliestTime: Long                        = creationTime
  override def valueAt(time: Long): Option[Any] = if (time >= earliestTime) Some(value) else None

  override def valuesAfter(time: Long): Array[Any] =
    if (time >= earliestTime) Array[Any](value) else Array[Any]()

  override def update(msgTime: Long, newValue: Any): Unit =
    if (msgTime <= earliestTime) earliestTime = msgTime
  override def currentValue(): Any                        = value
  override def creation(): Long                           = earliestTime

  override def values(): Array[(Long, Any)] = Array[(Long, Any)]((creationTime, value))

}
