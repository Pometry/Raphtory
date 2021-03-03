package com.raphtory.core.model.graphentities

import scala.collection.mutable
import scala.collection.parallel.mutable.ParTrieMap

/** *
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
abstract class Entity(val creationTime: Long, isInitialValue: Boolean) {

  // Properties from that entity
  private var entityType: Option[String]               = None
  var properties: ParTrieMap[String, Property] = ParTrieMap[String, Property]()

  // History of that entity
  object HistoryOrdering extends Ordering[Long] {
    def compare(key1: Long, key2: Long) = key2.compareTo(key1)
  }
  var history: mutable.TreeMap[Long, Boolean]   = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)

  var oldestPoint: Long = creationTime
  var newestPoint: Long = creationTime

  // History of that entity
  def removeList: mutable.TreeMap[Long, Boolean] = history.filter(f=> !f._2)

  def setType(newType: Option[String]): Unit = newType.foreach(nt => entityType = entityType.orElse(Some(nt)))
  def getType: String                = entityType.getOrElse("")

  def revive(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    history += ((msgTime, true))
  }

  def kill(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    history += ((msgTime, false))
  }

  def checkOldestNewest(msgTime: Long) = {
    if (msgTime > newestPoint)
      newestPoint = msgTime
    if (oldestPoint > msgTime) //check if the current point in history is the oldest
      oldestPoint = msgTime
  }

  /** *
    * override the apply method so that we can do edge/vertex("key") to easily retrieve properties
    */
  def apply(property: String): Property = properties(property)



  /** *
    * Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
    */
  def +(msgTime: Long, immutable: Boolean, key: String, value: Any): Unit =
    properties.get(key) match {
      case Some(p) => {
        p update (msgTime, value)
      }
      case None =>
        if (immutable) properties.put(key, new ImmutableProperty(msgTime, value))
        else {
          properties.put(key, new MutableProperty(msgTime, value))
        }
    }

  def wipe() = history = mutable.TreeMap()(HistoryOrdering)


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

  def aliveAt(time: Long): Boolean =
    if (time < oldestPoint)
      false
    else {
      val closest = closestTime(time)
      closest._2
    }

  def aliveAtWithWindow(time: Long, windowSize: Long): Boolean =
    if (time < oldestPoint)
      false
    else {
      val closest = closestTime(time)
      if (time - closest._1 < windowSize)
        closest._2
      else false
    }

  def activityAfter(time:Long) = history.exists(k => k._1 >= time)
  def activityBefore(time:Long)= history.exists(k => k._1 <= time)
  def activityBetween(min:Long, max:Long)= history.exists(k => k._1 > min &&  k._1 <= max)


}

