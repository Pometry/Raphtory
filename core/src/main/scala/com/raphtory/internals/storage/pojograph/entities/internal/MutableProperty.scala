package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.SearchPoint
import com.raphtory.internals.storage.pojograph.History
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

  private val previousState: History[PropertyValue[Any]] = History()
  previousState.insert(PropertyValue(creationTime, index, value))

  private def earliest: PropertyValue[Any] = previousState.first
  override def creation: Long              = earliest.time

  def update(msgTime: Long, index: Long, value: Any): Unit = {
    val newValue = PropertyValue(msgTime, index, value)
    previousState.insert(newValue)
  }

  def valueAt(time: Long): collection.Iterable[Any] =
    previousState.slice(SearchPoint(time, Long.MinValue), SearchPoint(time + 1, Long.MinValue)).map(_.value)

  override def valueHistory(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Iterable[PropertyValue[Any]] =
    previousState.slice(SearchPoint(after, Long.MinValue), SearchPoint(before + 1, Long.MinValue))

  def currentValue: Any = previousState.last.value
  def currentTime: Long = previousState.last.time

  override def values: Iterable[PropertyValue[Any]] = previousState.buffer

}
