package com.raphtory.core.storage.pojograph.entities.internal

import scala.collection.mutable

/** @DoNotDocument
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
abstract class PojoEntity(val creationTime: Long, isInitialValue: Boolean) {

  // Properties from that entity
  private var entityType: String                = ""
  var properties: mutable.Map[String, Property] = mutable.Map[String, Property]()

  // History of that entity

  //var history: mutable.TreeMap[Long, Boolean] = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
  var history: mutable.ArrayBuffer[(Long, Boolean)] = mutable.ArrayBuffer()
  var deletions: mutable.ListBuffer[Long]           = mutable.ListBuffer.empty
  history += ((creationTime, isInitialValue))
  var toClean                                       = false

  def dedupe() =
    if (toClean) {
      history = history.distinct
      toClean = false
    }

  var oldestPoint: Long = creationTime

  // History of that entity
  def removeList: List[Long] = deletions.toList
  //.filter(f => if(!f._2) f._1).toList

  def setType(newType: Option[String]): Unit =
    newType match {
      case Some(t) => entityType = t
      case None    =>
    }

  def getType: String = entityType

  def revive(msgTime: Long): Unit = {
    checkOldestTime(msgTime)
    history += ((msgTime, true))
    toClean = true
  }

  def kill(msgTime: Long): Unit = {
    checkOldestTime(msgTime)
    history += ((msgTime, false))
    deletions += msgTime
    toClean = true
  }

  def checkOldestTime(msgTime: Long) =
    if (oldestPoint > msgTime) //check if the current point in history is the oldest
      oldestPoint = msgTime

  // override the apply method so that we can do edge/vertex("key") to easily retrieve properties
  def apply(property: String): Property = properties(property)

  // Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
  def +(msgTime: Long, immutable: Boolean, key: String, value: Any): Unit =
    properties.get(key) match {
      case Some(p) =>
        p update (msgTime, value)
      case None    =>
        if (immutable) properties.put(key, new ImmutableProperty(msgTime, value))
        else
          properties.put(key, new MutableProperty(msgTime, value))
    }

  def wipe() = history = mutable.ArrayBuffer()

  protected def closestTime(time: Long): (Long, Boolean) = {
    var closestTime: Long = -1
    var value             = false
    for ((k, v) <- history)
      if (k <= time)
        if ((time - k) < (time - closestTime)) {
          closestTime = k
          value = v
        }
    (closestTime, value)
  }

  def aliveAt(time: Long): Boolean = if (time < oldestPoint) false else closestTime(time)._2

  def aliveBetween(startTime: Long, endTime: Long): Boolean =
    if (endTime < oldestPoint)
      false
    else {
      val closest = closestTime(endTime)
      if (startTime < closest._1)
        closest._2
      else false
    }

  def activityAfter(time: Long)             = history.exists(k => k._1 >= time)
  def activityBefore(time: Long)            = history.exists(k => k._1 <= time)
  def activityBetween(min: Long, max: Long) = history.exists(k => k._1 > min && k._1 <= max)

}
