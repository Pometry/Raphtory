package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.HistoryOrdering

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
abstract class Entity(val creationTime: Long, isInitialValue: Boolean, storage: EntityStorage) {

  // Properties from that entity
  private var entityType: String               = null
  var properties: ParTrieMap[String, Property] = ParTrieMap[String, Property]()

  // History of that entity
  var history: mutable.TreeMap[Long, Boolean]   = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
  var compressedState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
  private var saved                                   = false
  var shouldBeWiped                                   = false

  var oldestPoint: Long = creationTime
  var newestPoint: Long = creationTime

  // History of that entity
  var removeList: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
  if (!isInitialValue)
    removeList = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)

  def setType(newType: String): Unit = if (entityType == (null)) entityType = newType
  def getType: String                = if (entityType == null) "" else entityType

  def revive(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    history.put(msgTime, true)
  }

  def kill(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    removeList.put(msgTime, false)
    history.put(msgTime, false)
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

  def compressHistory(cutoff: Long): mutable.TreeMap[Long, Boolean] = {
    if (history.isEmpty)
      return mutable.TreeMap()(HistoryOrdering) //if we have no need to compress, return an empty list
    if (history.takeRight(1).head._1 >= cutoff)
      return mutable.TreeMap()(HistoryOrdering) //if the oldest point is younger than the cut off no need to do anything
    var toWrite: mutable.TreeMap[Long, Boolean]          = mutable.TreeMap()(HistoryOrdering) //data which needs to be saved to cassandra
    var newPreviousState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering) //map which will replace the current history

    var PriorPoint: (Long, Boolean) = history.head
    var swapped                     = false //specify pivot point when crossing over the cutoff as you don't want to compare to historic points which are still changing
    history.foreach {
      case (k, v) =>
        if (k >= cutoff)                                      //still in read write space so
          newPreviousState.put(k, v)                          //add to new uncompressed history
        else {                                                //reached the history which should be compressed
          if (swapped && (v != PriorPoint._2)) {              //if we have skipped the pivot and the types are different
            toWrite.put(PriorPoint._1, PriorPoint._2)         //add to toWrite so this can be saved to cassandra
            compressedState.put(PriorPoint._1, PriorPoint._2) //add to compressedState in-mem
          }
          swapped = true
        }
        PriorPoint = (k, v)
    }
    if ((PriorPoint._1 < cutoff)) {                                                //
      if (compressedState.isEmpty || (compressedState.head._2 != PriorPoint._2)) { //if the last point is before the cut off --if compressedState is empty or the head of the compressed state is different to the final point of the uncompressed state then write
        toWrite.put(PriorPoint._1, PriorPoint._2)                                  //add to toWrite so this can be saved to cassandra
        compressedState.put(PriorPoint._1, PriorPoint._2)                          //add to compressedState in-mem
      }
    } else
      newPreviousState.put(
              PriorPoint._1,
              PriorPoint._2
      ) //if the last point in the uncompressed history wasn't past the cutoff it needs to go back into the new uncompressed history
    history = newPreviousState
    toWrite
  }

  //val removeFrom = if(compressing) compressedState else previousState
  def archive(cutoff: Long, compressing: Boolean, entityType: Boolean, workerID: Int): Boolean = { //
    if (history.isEmpty && compressedState.isEmpty) return false //blank node, decide what to do later
    if (compressedState.nonEmpty) {
      if (compressedState.takeRight(1).head._1 >= cutoff)
        return false //if the oldest point is younger than the cut off no need to do anything
      var head                                               = compressedState.head._2 //get the head of later
      val newCompressedState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
      compressedState.foreach {
        case (k, v) =>
          if (k >= cutoff)
            newCompressedState put (k, v)
          //else recordRemoval(entityType, workerID)
      }                                    //for each point if it is safe then keep it
      compressedState = newCompressedState //overwrite the compressed state
      //properties.foreach{case ((propkey, property)) =>{property.archive(cutoff,compressing,entityType)}}//do the same for all properties //currently properties should exist until entity removed
      newestPoint < cutoff && !head //return all points older than cutoff and latest update is deletion
    }
    false
  }

  def archiveOnly(cutoff: Long, entityType: Boolean, workerID: Int): Boolean = { //
    if (history.isEmpty) return false //blank node, decide what to do later
    if (history.takeRight(1).head._1 >= cutoff)
      return false //if the oldest point is younger than the cut off no need to do anything

    var head                                             = history.head._2 //get the head of later
    val newPreviousState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
    history.foreach {
      case (k, v) =>
        if (k >= cutoff)
          newPreviousState put (k, v)
        //else recordRemoval(entityType, workerID)
    }                                 //for each point if it is safe then keep it
    history = newPreviousState  //overwrite the compressed state
    newestPoint < cutoff && !head //return all points older than cutoff and latest update is deletion
    false
  }

  /** *
    * Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
    *
    * @param msgTime Message ID where the update came from
    * @param key   property key
    * @param value property value
    */
  def +(msgTime: Long, immutable: Boolean, key: String, value: Any): Unit =
    properties.get(key) match {
      case Some(p) => p update (msgTime, value)
      case None =>
        if (immutable) properties.put(key, new MutableProperty(msgTime, key, value, storage))
        else properties.put(key, new ImmutableProperty(msgTime, key, value, storage))
    }

  def beenSaved(): Boolean = saved
  def firstSave(): Unit    = saved = true

  def wipe() = history = mutable.TreeMap()(HistoryOrdering)

  def getHistorySize(): Int = history.size + compressedState.size

  def getUncompressedSize(): Int = history.size

  def getId: Long
  def getPropertyCurrentValue(key: String): Option[Any] =
    properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None    => None
    }

  protected val stateCache = ParTrieMap[Long, Long]()

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
      if (time - closest._1 <= windowSize)
        closest._2
      else false
    }

  def activityAfter(time:Long) = history.exists(k => k._1 >= time)
  def activityBefore(time:Long)= history.exists(k => k._1 <= time)
  def activityBetween(min:Long, max:Long)= history.exists(k => k._1 >= min &&  k._1 <= max)


}
//def latestRouterCheck(newRouter:Int):Boolean = newRouter==latestRouter
//def updateLatestRouter(newRouter:Int):Unit = latestRouter = newRouter
