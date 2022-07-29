package com.raphtory.internals.storage.pojograph.entities

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.TimePoint
import com.raphtory.internals.storage.pojograph.entities.internal.ImmutableProperty
import com.raphtory.internals.storage.pojograph.entities.internal.MutableProperty
import com.raphtory.internals.storage.pojograph.entities.internal.Property
import com.raphtory.internals.storage.pojograph.entities.internal.PropertyView
import com.raphtory.internals.storage.pojograph.History
import com.raphtory.internals.storage.pojograph.HistoryOps
import com.raphtory.internals.storage.pojograph.HistoryView
import com.raphtory.utils.OrderingFunctions.max
import com.raphtory.utils.OrderingFunctions.min

import scala.collection.mutable

trait PojoEntityView {
  val historyView: HistoryOps[HistoricEvent]

  def oldestPoint: Long = historyView.first.time
  def latestPoint: Long = historyView.last.time

  def Type: String

  // override the apply method so that we can do edge/vertex("key") to easily retrieve properties
  def apply(property: String): PropertyView = get(property).get

  def get(property: String): Option[PropertyView]

  def aliveAt(time: Long): Boolean =
    aliveAt(TimePoint.last(time))

  def aliveAt(time: IndexedValue): Boolean =
    historyView.closest(time) match {
      case Some(h) => h.event
      case None    => false
    }

  // This function considers that the start of the window is exclusive, but PojoEntity.aliveBetween
  // considers both bounds as inclusive, we correct the start of the window by 1
  def aliveAt(time: Long, window: Long): Boolean =
    aliveBetween(time - window + 1, time)

  def aliveBetween(startTime: Long, endTime: Long): Boolean =
    aliveBetween(TimePoint.first(startTime), TimePoint.last(endTime))

  // startTime and endTime are inclusive. The existence of exclusive bounds is handled externally
  def aliveBetween(startTime: IndexedValue, endTime: IndexedValue): Boolean =
    historyView.closest(endTime) match {
      case None    => false
      case Some(h) =>
        if (startTime <= h)
          h.event // TODO: Check the logic for what should happen if entity is alive for part of the window
        else false
    }

  def activityAfter(time: Long): Boolean  = historyView.after(TimePoint(time, Long.MinValue)).nonEmpty
  def activityBefore(time: Long): Boolean = historyView.before(TimePoint(time, Long.MaxValue)).nonEmpty

  def active(after: Long, before: Long): Boolean =
    active(TimePoint.first(after), TimePoint.last(before))

  def active(after: IndexedValue, before: IndexedValue): Boolean =
    historyView.slice(after, before.next).nonEmpty
}
