package com.raphtory.core.model.graphentities


import com.raphtory.core.utils.HistoryOrdering
import monix.execution.atomic.AtomicLong

import scala.collection.parallel.mutable.ParTrieMap
import scala.collection.mutable

/** *
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */

abstract class Entity(val creationTime: Long, isInitialValue: Boolean, addOnly: Boolean) {

  // Properties from that entity
  var properties:ParTrieMap[String,Property] = ParTrieMap[String, Property]()

  // History of that entity
  var previousState : mutable.TreeMap[Long, Boolean] = null
  if (!addOnly)
    previousState = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)

  //track the oldest point for use in AddOnly mode
  var oldestPoint : AtomicLong=  AtomicLong(creationTime)

  // History of that entity
  var removeList: mutable.TreeMap[Long,Boolean] = null

  if(isInitialValue)
    removeList = mutable.TreeMap()(HistoryOrdering)
  else
    removeList = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
  /** *
    * Set the Entity has alive at a given time
    *
    * @param msgTime
    */
  def revive(msgTime: Long): Unit = {
    if(addOnly) {
      // if we are in add only mode
      if (oldestPoint.get > msgTime) //check if the current point in history is the oldest
        oldestPoint.set(msgTime)
    }
    else {
      previousState.put(msgTime, true)
    }
  }

  /** *
    * Set the entity absent in a given time
    *
    * @param msgTime
    */
  def kill(msgTime: Long): Unit = {
    removeList.put(msgTime, false)
    if (!addOnly)
      previousState.put(msgTime, false)
  }

  /** *
    * override the apply method so that we can do edge/vertex("key") to easily retrieve properties
    *
    * @param property property key
    * @return property value
    */
  def apply(property: String): Property = properties(property)

  def compressAndReturnOldHistory(cutoff:Long): mutable.TreeMap[Long, Boolean] ={
    if(getPreviousStateSize==0){ //if the state size is 0 it is a wiped node and should not be interacted with
      return  mutable.TreeMap()(HistoryOrdering) //if the size is one, no need to compress
    }
    var safeHistory : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
    var oldHistory : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)

    val head = previousState.head
    safeHistory += head // always keep at least one point in history

    var prev: (Long,Boolean) = head
    var swapped = false
    for((k,v) <- previousState){
      if(k<cutoff) {
        if(swapped) //as we are adding prev skip the value on the pivot as it is already added
          if (v == !prev._2) {
            oldHistory += prev //if the current point differs from the val of the previous it means the prev was the last of its type
            safeHistory += prev
          }
        swapped=true
      }
      else
        safeHistory += k -> v
      prev = (k,v)
    }

    if(prev._1<cutoff) {
      safeHistory += prev
      oldHistory += prev //add the final history point to oldHistory as not done in loop
    }
    previousState = safeHistory
    oldHistory
  }

  /** *
    * check what part of the history is outside of the historians time window
    *
    * @param cutoff the histories time cutoff
    * @return (is it a place holder, is the full history holder than the cutoff, the old history)
    */
  def returnAncientHistory(cutoff:Long): (Boolean, Boolean, mutable.TreeMap[Long, Boolean]) ={ //
    if(getPreviousStateSize==0){ //if the state size is 0 it is a wiped node inform the historian
      return  (true,true,null)
    }
    var safeHistory : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering )
    var oldHistory : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)

    var allOld = true
    safeHistory += previousState.head // always keep at least one point in history
    for((k,v) <- previousState){
      if(k<cutoff)
        oldHistory += k ->v
      else {
        allOld =false
        safeHistory += k -> v
      }
    }
    previousState = safeHistory

    (false,allOld,oldHistory)
  }

  /** *
    * Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
    *
    * @param msgTime Message ID where the update came from
    * @param key   property key
    * @param value property value
    */
  def +(msgTime: Long, key: String, value: String): Unit = {
    properties.get(key) match {
      case Some(v) => v update(msgTime, value)
      case None => properties.put(key, new Property(msgTime, key, value))
    }
  }

  def updateProp(key: String, p : Property) = {
    properties.synchronized {
      properties.get(key) match {
        case Some(v) => v update(p.currentTime, p.currentValue)
        case None => properties.put(key, p)
      }
    }
  }

  //************* PRINT ENTITY DETAILS BLOCK *********************\\
/*  def printCurrent(): String = {
    var toReturn = s"MessageID ${previousState.head._1}: ${previousState.head._2} " + System.lineSeparator
    properties.foreach(p =>
      toReturn = s"$toReturn      ${p._2.toStringCurrent} " + System.lineSeparator)
    toReturn
    ""
  }

  /** *
    * Returns string with previous state of entity + properties -- title left off as will be done in subclass
    *
    * @return
    */
  def printHistory(): String = {
    var toReturn = "Previous state of entity: " + System.lineSeparator
    if (!addOnly)
      previousState.foreach(p =>
       toReturn = s"$toReturn MessageID ${p._1}: ${p._2} " + System.lineSeparator)
      s"$toReturn \n $printProperties"
      ""
  }

  def printProperties(): String = { //test function to make sure the properties are being added to the correct vertices
    var toReturn = "" //indent to be inside the entity
    properties.toSeq
      .sortBy(_._1)
      .foreach(p => toReturn = s"$toReturn      ${p._2.toString} \n")
    toReturn
  }*/

  //************* END PRINT ENTITY DETAILS BLOCK *********************\\

  def wipe() = {
    if (addOnly)
      oldestPoint.set(Long.MaxValue)
    else
      previousState = mutable.TreeMap()(HistoryOrdering)
  }

  def getPreviousStateSize() : Int = {
    if (addOnly)
      if(oldestPoint.get == Long.MaxValue)
        0 //is a placeholder entity
      else
        1
    else
      previousState.size
  }

  def getId : Long
  def getPropertyCurrentValue(key : String) : Option[String] =
    properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None => None
    }
}
