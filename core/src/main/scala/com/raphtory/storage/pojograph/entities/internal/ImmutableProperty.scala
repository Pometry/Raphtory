package com.raphtory.storage.pojograph.entities.internal

class ImmutableProperty(creationTime: Long, value: Any) extends Property {
  private var earliestTime: Long                = creationTime
  override def valueAt(time: Long): Option[Any] = if (time >= earliestTime) Some(value) else None

  override def valueHistory(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Array[(Long, Any)] =
    if (earliestTime > before)
      Array.empty[(Long, Any)]
    else if (after > earliestTime)
      Array[(Long, Any)]((after, value))
    else
      Array[(Long, Any)]((earliestTime, value))

  override def update(msgTime: Long, newValue: Any): Unit =
    if (msgTime <= earliestTime) earliestTime = msgTime
  override def currentValue(): Any                        = value
  override def creation(): Long                           = earliestTime

  override def values(): Array[(Long, Any)] = Array[(Long, Any)]((creationTime, value))

}
