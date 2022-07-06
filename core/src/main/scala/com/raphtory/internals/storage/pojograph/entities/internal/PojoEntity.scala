package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.api.analysis.visitor.IndexedValue
import com.raphtory.api.analysis.visitor.PropertyValue
import com.raphtory.api.analysis.visitor.SearchPoint
import com.raphtory.internals.storage.pojograph.OrderedBuffer._

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.IndexedSeqView
import scala.collection.mutable
import com.raphtory.internals.storage.pojograph.History

import scala.math.Ordering.Implicits.infixOrderingOps

case class Index[T: Ordering](time: Long, secondaryIndex: T)

/** Represents Graph Entities (Edges and Vertices)
  *
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclasses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
abstract private[raphtory] class PojoEntity(
    val creationTime: Long,
    val index: Long,
    isInitialValue: Boolean
) {

  // Properties from that entity
  private var entityType: String                = ""
  val properties: mutable.Map[String, Property] = mutable.Map[String, Property]()

  // History of that entity
  val history: History[HistoricEvent]             = History()
  history.insert(HistoricEvent(creationTime, index, isInitialValue))
  var deletions: mutable.ListBuffer[(Long, Long)] = mutable.ListBuffer.empty

  def oldestPoint: Long = history.first.time
  def latestPoint: Long = history.last.time

  def viewHistoryBetween(after: Long, before: Long): IndexedSeqView[HistoricEvent] =
    history.slice(SearchPoint(after, Long.MinValue), SearchPoint(before, Long.MaxValue))

  // History of that entity
  def deletionList: List[(Long, Long)] = deletions.toList

  def setType(newType: Option[String]): Unit =
    newType match {
      case Some(t) => entityType = t
      case None    =>
    }

  def getType: String = entityType

  def revive(msgTime: Long, index: Long): Unit =
    history insert HistoricEvent(msgTime, index, event = true)

  def kill(msgTime: Long, index: Long): Unit = {
    history insert HistoricEvent(msgTime, index, event = false)
    deletions sortedAppend (msgTime, index)
  }

  // override the apply method so that we can do edge/vertex("key") to easily retrieve properties
  def apply(property: String): Property = properties(property)

  // Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
  def +(msgTime: Long, index: Long, immutable: Boolean, key: String, value: Any): Unit =
    properties.get(key) match {
      case Some(p) =>
        p update (msgTime, index, value)
      case None    =>
        if (immutable) properties.put(key, new ImmutableProperty(msgTime, index, value))
        else
          properties.put(key, new MutableProperty(msgTime, index, value))
    }

  def wipe(): Unit = history.clear()

  def aliveAt(time: Long): Boolean =
    history.closest(time, Long.MaxValue) match {
      case Some(h) => h.event
      case None    => false
    }

  // startTime and endTime are inclusive. The existence of exclusive bounds is handled externally
  def aliveBetween(startTime: Long, endTime: Long): Boolean =
    history.closest(endTime, Long.MaxValue) match {
      case None    => false
      case Some(h) =>
        if (startTime <= h.time)
          h.event // TODO: Check the logic for what should happen if entity is alive for part of the window
        else false
    }

  def activityAfter(time: Long): Boolean  = history.after(SearchPoint(time, Long.MinValue)).nonEmpty
  def activityBefore(time: Long): Boolean = history.before(SearchPoint(time, Long.MaxValue)).nonEmpty

  def activityBetween(min: Long, max: Long): Boolean =
    history.slice(SearchPoint(min, Long.MinValue), SearchPoint(max, Long.MaxValue)).nonEmpty

}
