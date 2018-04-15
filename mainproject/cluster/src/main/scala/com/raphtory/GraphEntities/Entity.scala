package com.raphtory.GraphEntities

import java.util.concurrent.atomic.AtomicInteger

import com.raphtory.utils.HistoryOrdering
import monix.execution.atomic.AtomicLong

import scala.collection.concurrent.TrieMap
import scala.collection.{SortedMap, mutable}

/** *
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */
class Entity(creationTime: Long, isInitialValue: Boolean, addOnly: Boolean) {

  // Properties from that entity
  var properties:TrieMap[String,Property] = TrieMap[String, Property]()

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
    else
      previousState += msgTime -> true
  }

  /** *
    * Set the entity absent in a given time
    *
    * @param msgTime
    */
  def kill(msgTime: Long): Unit = {
    removeList    += msgTime -> false
    if (!addOnly)
      previousState += msgTime -> false
  }

  /** *
    * override the apply method so that we can do edge/vertex("key") to easily retrieve properties
    *
    * @param property property key
    * @return property value
    */
  def apply(property: String): Property = properties(property)

  def removeAndReturnOldHistory(cutoff:Long): mutable.TreeMap[Long, Boolean] ={
    val (safeHistory, oldHistory) = historySplit(cutoff)
    previousState = safeHistory
    oldHistory
  }

  def historySplit(cutOff: Long):( mutable.TreeMap[Long, Boolean], mutable.TreeMap[Long, Boolean]) = {

    if(getPreviousStateSize==0){
     return (previousState,null)
    }

    var safeHistory : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
    var oldHistory : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
    val head = previousState.head
    println(head)
    safeHistory += head // always keep at least one point in history

    var prev: (Long,Boolean) = head
    previousState.head
    for((k,v) <- previousState){
      if(k<cutOff)
       if(v == !prev._2)
          oldHistory += prev._1 -> prev._2 //if the current point differs from the val of the previous it means the prev was the last of its type
      else
        safeHistory += k -> v
      prev = (k,v)
    }
    println(prev._1)
    if(prev._1<cutOff)
      oldHistory += prev //add the final history point to oldHistory as not done in loop
    (safeHistory,oldHistory)
  }
  /** *
    * Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
    *
    * @param msgTime Message ID where the update came from
    * @param key   property key
    * @param value property value
    */
  def +(msgTime: Long, key: String, value: String): Unit = {
    properties.putIfAbsent(key, new Property(msgTime, key, value)) match {
      case Some(oldValue) => oldValue update(msgTime, value)
      case None =>
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
    if (!addOnly)
      previousState = mutable.TreeMap()(HistoryOrdering)
  }

  def getPreviousStateSize() : Int = {
    if (addOnly)
      0
    else
      previousState.size
  }
}
