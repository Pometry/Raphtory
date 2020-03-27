package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.HistoryOrdering

import scala.collection.mutable

/** *
  * Node or Vertice Property. Created by Mirate on 10/03/2017.
  *
  * @param creationTime
  * @param key           Property name
  * @param value         Property value
  */
class MutableProperty(creationTime: Long, key: String, value: Any, storage: EntityStorage) extends Property {
  private var saved = false

  // Initialize the TreeMap
  var previousState: mutable.TreeMap[Long, Any]   = mutable.TreeMap()(HistoryOrdering)
  var compressedState: mutable.TreeMap[Long, Any] = mutable.TreeMap()(HistoryOrdering)

  // add in the initial information
  update(creationTime, value)
  def name = key

  def update(msgTime: Long, newValue: Any): Unit =
    previousState.put(msgTime, newValue)

  def compressHistory(cutoff: Long, entityType: Boolean, workerID: Int): mutable.TreeMap[Long, Any] = {
    if (previousState.isEmpty)
      return mutable.TreeMap()(HistoryOrdering) //if we have no need to compress, return an empty list
    if (previousState.takeRight(1).head._1 >= cutoff)
      return mutable.TreeMap()(HistoryOrdering) //if the oldest point is younger than the cut off no need to do anything
    var toWrite: mutable.TreeMap[Long, Any]          = mutable.TreeMap()(HistoryOrdering) //data which needs to be saved to cassandra
    var newPreviousState: mutable.TreeMap[Long, Any] = mutable.TreeMap()(HistoryOrdering) //map which will replace the current history

    var PriorPoint: (Long, Any) = previousState.head
    var swapped                 = false //specify pivot point when crossing over the cutoff as you don't want to compare to historic points which are still changing
    previousState.foreach {
      case (k, v) =>
        if (k >= cutoff)                                      //still in read write space so
          newPreviousState.put(k, v)                          //add to new uncompressed history
        else {                                                //reached the history which should be compressed
          if (swapped && (v != PriorPoint._2)) {              //if we have skipped the pivot and the types are different
            toWrite.put(PriorPoint._1, PriorPoint._2)         //add to toWrite so this can be saved to cassandra
            compressedState.put(PriorPoint._1, PriorPoint._2) //add to compressedState in-mem
          } else
            recordRemoval(entityType, workerID)
          swapped = true
        }
        PriorPoint = (k, v)
    }
    if ((PriorPoint._1 < cutoff))                                                  //
      if (compressedState.isEmpty || (compressedState.head._2 != PriorPoint._2)) { //if the last point is before the cut off --if compressedState is empty or the head of the compressed state is different to the final point of the uncompressed state then write
        toWrite.put(PriorPoint._1, PriorPoint._2)                                  //add to toWrite so this can be saved to cassandra
        compressedState.put(PriorPoint._1, PriorPoint._2)                          //add to compressedState in-mem
      } else
        recordRemoval(entityType, workerID)
    else
      newPreviousState.put(
              PriorPoint._1,
              PriorPoint._2
      ) //if the last point in the uncompressed history wasn't past the cutoff it needs to go back into the new uncompressed history
    previousState = newPreviousState
    toWrite
  }

  def recordRemoval(entityType: Boolean, workerID: Int) =
    if (entityType)
      storage.vertexPropertyDeletionCount(workerID) += 1
    else
      storage.edgePropertyDeletionCount(workerID) += 1

  def getPreviousStateSize(): Int =
    previousState.size

  override def equals(obj: scala.Any): Boolean = {
    if (obj.isInstanceOf[MutableProperty]) {
      val prop2 = obj.asInstanceOf[MutableProperty]
      if (name.equals(prop2.name) && (previousState.equals(prop2.previousState)))
        return true
      return false
    }
    false
  }

  def valueAt(time: Long): Any = {
    var closestTime: Long = 0
    var value: Any        = "Default"
    for ((k, v) <- compressedState)
      if (k <= time)
        if ((time - k) < (time - closestTime)) {
          closestTime = k
          value = v
        }
    value
  }

  override def toString: String =
    s"History: $previousState"
  // var toReturn = System.lineSeparator()
  // previousState.foreach(p =>
  //   toReturn = s"$toReturn           MessageID ${p._1}: ${p._2} -- ${p._2} " + System
  //     .lineSeparator())
  // s"Property: ${key} ----- Previous State: $toReturn"

  def toStringCurrent: String = {
    val toReturn = System.lineSeparator() +
      s"           MessageID ${previousState.head._1}: ${previousState.head._2} -- ${previousState.head._2} " +
      System.lineSeparator()
    s"Property: $key ----- Current State: $toReturn"
  }

  def currentValue: Any    = previousState.head._2
  def currentTime: Long    = previousState.head._1
  def beenSaved(): Boolean = saved

}
