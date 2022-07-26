package com.raphtory.internals.storage.pojograph.entities.internal

import com.raphtory.api.analysis.visitor.HistoricEvent
import com.raphtory.internals.storage.pojograph.OrderedBuffer._

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint
import scala.collection.mutable

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
  var history: mutable.ArrayBuffer[HistoricEvent] = mutable.ArrayBuffer.empty
  var deletions: mutable.ListBuffer[(Long, Long)] = mutable.ListBuffer.empty
  history sortedAppend HistoricEvent(creationTime, index, isInitialValue)

  def oldestPoint: Long = history(0).time
  def latestPoint: Long = history(history.length - 1).time

  // History of that entity
  def deletionList: List[(Long, Long)] = deletions.toList

  def setType(newType: Option[String]): Unit =
    newType match {
      case Some(t) => entityType = t
      case None    =>
    }

  def getType: String = entityType

  def revive(msgTime: Long, index: Long): Unit =
    history sortedAppend HistoricEvent(msgTime, index, event = true)

  def kill(msgTime: Long, index: Long): Unit = {
    history sortedAppend HistoricEvent(msgTime, index, event = false)
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

  def wipe(): Unit = history = mutable.ArrayBuffer()

  protected def closestTime(time: Long, index: Long): HistoricEvent =
    if (time < oldestPoint)
      HistoricEvent(-1, -1, false)
    else {
      val point = history.search(HistoricEvent(time, index, true))
      point match {
        case Found(i)          => history(i)
        case InsertionPoint(i) => history(i - 1)
      }
    }

  def aliveAt(time: Long): Boolean = if (time < oldestPoint) false else closestTime(time, Long.MaxValue).event

  // startTime and endTime are inclusive. The existence of exclusive bounds is handled externally
  def aliveBetween(startTime: Long, endTime: Long): Boolean =
    if (endTime < oldestPoint)
      false
    else {
      val closest = closestTime(endTime, Long.MaxValue)
      if (startTime <= closest.time)
        closest.event // TODO: Check the logic for what should happen if entity is alive for part of the window
      else false
    }

  def activityAfter(time: Long)             = history.exists(k => k.time >= time)
  def activityBefore(time: Long)            = history.exists(k => k.time <= time)
  def activityBetween(min: Long, max: Long) = history.exists(k => k.time >= min && k.time <= max)

}
