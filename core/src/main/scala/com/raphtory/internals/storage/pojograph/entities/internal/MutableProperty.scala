package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.History
import com.raphtory.internals.storage.pojograph.HistoryOps
import com.raphtory.internals.storage.pojograph.HistoryView
import com.raphtory.internals.storage.pojograph.OrderedBuffer._

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable
import scala.math.Ordering.Implicits.infixOrderingOps

private[raphtory] trait MutablePropertyBase extends PropertyView {
  protected val propertyHistory: HistoryOps[PropertyValue[Any]]
  def earliest: PropertyValue[Any] = propertyHistory.first

  def valueAt(time: IndexedValue): collection.Iterable[Any] =
    propertyHistory.slice(time, time.next).map(_.value)

  override def valueHistory(
      after: IndexedValue,
      before: IndexedValue
  ): Iterable[PropertyValue[Any]] =
    propertyHistory.slice(after, before.next)

  def currentValue: PropertyValue[Any] = propertyHistory.last

  override def values: Iterable[PropertyValue[Any]] = propertyHistory

  override def viewBetween(after: IndexedValue, before: IndexedValue): PropertyView =
    if (after <= earliest && before >= currentValue) this
    else new MutablePropertyView(propertyHistory.slice(after, before.next))
}

private[raphtory] class MutablePropertyView(override val propertyHistory: HistoryOps[PropertyValue[Any]])
        extends MutablePropertyBase

/**
  * @param creationTime
  * @param value         Property value
  */
private[raphtory] class MutableProperty(creationTime: Long, index: Long, value: Any)
        extends Property
        with MutablePropertyBase {
  override val propertyHistory: History[PropertyValue[Any]] = History()
  propertyHistory.insert(PropertyValue(creationTime, index, value))

  def update(msgTime: Long, index: Long, value: Any): Unit = {
    val newValue = PropertyValue(msgTime, index, value)
    propertyHistory.insert(newValue)
  }

}
