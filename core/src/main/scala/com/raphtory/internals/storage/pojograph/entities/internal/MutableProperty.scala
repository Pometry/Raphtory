package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.internals.storage.pojograph.OrderedBuffer._

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable
import scala.math.Ordering.Implicits.infixOrderingOps

/**
  * @param creationTime
  * @param value         Property value
  */
private[raphtory] class MutableProperty(creationTime: Long, index: Long, value: Any) extends Property {

  private val previousState: mutable.ArrayBuffer[PropertyValue[Any]] =
    mutable.ArrayBuffer(PropertyValue(creationTime, index, value))

  private def earliest: PropertyValue[Any] = previousState.head
  override def creation: Long              = earliest.time

  def update(msgTime: Long, index: Long, value: Any): Unit = {
    val newValue = PropertyValue(msgTime, index, value)
    previousState.sortedAppend(newValue)
  }

  def valueAt(time: Long): collection.Iterable[Any] =
    if (time < earliest.time)
      Seq.empty
    else {
      val start = previousState.search(PropertyValue[Any](time, Long.MinValue)).insertionPoint
      val end   = previousState.search(PropertyValue[Any](time, Long.MaxValue), start, previousState.size) match {
        case Found(i)          => i + 1
        case InsertionPoint(i) => i
      }
      previousState.slice(start, end).map(_.value)
    }

  override def valueHistory(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Iterable[PropertyValue[Any]] =
    previousState.filter(x => (x.time >= after) && (x.time <= before))

  def currentValue: Any = previousState.head.value
  def currentTime: Long = previousState.head.time

  override def values: Iterable[PropertyValue[Any]] = previousState

}
