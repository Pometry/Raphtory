package com.raphtory.core.model.graphentities

import com.raphtory.core.actors.partitionmanager._
import com.raphtory.core.utils.HistoryOrdering
import monix.execution.atomic.AtomicLong

import scala.collection.parallel.mutable.ParTrieMap
import scala.collection.mutable
import spray.json._

/** *
  * Represents Graph Entities (Edges and Vertices)
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  *
  * @param creationTime ID of the message that created the entity
  * @param isInitialValue  Is the first moment this entity is referenced
  */

abstract class Entity(var latestRouter:Int, val creationTime: Long, isInitialValue: Boolean, addOnly: Boolean) {

  // Properties from that entity
  var properties:ParTrieMap[String,Property] = ParTrieMap[String, Property]()

  // History of that entity
  var previousState : mutable.TreeMap[Long, Boolean] = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
  var compressedState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
  private var saved = false
  var shouldBeWiped = false

  //track the oldest point for use in AddOnly mode
  var oldestPoint : AtomicLong=  AtomicLong(creationTime)
  var newestPoint: AtomicLong = AtomicLong(creationTime)
  var originalHistorySize : AtomicLong=  AtomicLong(0)

  // History of that entity
  var removeList: mutable.TreeMap[Long,Boolean] = mutable.TreeMap()(HistoryOrdering)
  if(!isInitialValue)
    removeList = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
  /** *
    * Set the Entity has alive at a given time
    *
    * @param msgTime
    */
  def revive(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    originalHistorySize.add(1)
    previousState.put(msgTime, true)
  }

  /** *
    * Set the entity absent in a given time
    *
    * @param msgTime
    */
  def kill(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    originalHistorySize.add(1)
    removeList.put(msgTime, false)
    previousState.put(msgTime, false)

  }

  def checkOldestNewest(msgTime:Long) ={
    if(msgTime>newestPoint.get)
      newestPoint.set(msgTime)
    if (oldestPoint.get > msgTime) //check if the current point in history is the oldest
      oldestPoint.set(msgTime)
  }

  /** *
    * override the apply method so that we can do edge/vertex("key") to easily retrieve properties
    *
    * @param property property key
    * @return property value
    */
  def apply(property: String): Property = properties(property)

  def compressHistory(cutoff:Long): mutable.TreeMap[Long, Boolean] ={
    if(previousState.isEmpty) return mutable.TreeMap()(HistoryOrdering) //if we have no need to compress, return an empty list
    var toWrite : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering) //data which needs to be saved to cassandra
    var newPreviousState : mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering) //map which will replace the current history

    var PriorPoint: (Long,Boolean) = previousState.head
    var swapped = false //specify pivot point when crossing over the cutoff as you don't want to compare to historic points which are still changing
    previousState.foreach{case (k,v)=>{
      if(k >= cutoff) //still in read write space so
        newPreviousState.put(k,v) //add to new uncompressed history
      else { //reached the history which should be compressed
        if(swapped && (v != PriorPoint._2)) { //if we have skipped the pivot and the types are different
          toWrite.put(PriorPoint._1, PriorPoint._2) //add to toWrite so this can be saved to cassandra
          compressedState.put(PriorPoint._1, PriorPoint._2) //add to compressedState in-mem
        }
        swapped = true
      }
      PriorPoint = (k, v)
    }}
    if((PriorPoint._1<cutoff)) { //
      if (compressedState.isEmpty || (compressedState.head._2 != PriorPoint._2)) //if the last point is before the cut off --if compressedState is empty or the head of the compressed state is different to the final point of the uncompressed state then write
        toWrite.put(PriorPoint._1, PriorPoint._2) //add to toWrite so this can be saved to cassandra
    }
    else
      newPreviousState.put(PriorPoint._1,PriorPoint._2) //if the last point in the uncompressed history wasn't past the cutoff it needs to go back into the new uncompressed history
    previousState = newPreviousState
    toWrite
  }

  def compressionRate():Double ={
    previousState.size.toDouble/originalHistorySize.get.toDouble
  }

  /** *
    * check what part of the history is outside of the historians time window
    *
    * @param cutoff the histories time cutoff
    * @return (is it a place holder, is the full history holder than the cutoff, the old history)
    */
  //val removeFrom = if(compressing) compressedState else previousState
  def removeAncientHistory(cutoff:Long,compressing:Boolean): (Boolean, Boolean,Int,Int)={ //
    if(getHistorySize==0){ //if the state size is 0 it is a wiped node inform the historian
      return  (true,true,0,0)
    }
    var removed = 0
    var propRemoval = 0

    var head = false
    if(!removeFrom.isEmpty)
      head = removeFrom.head._2
    for((k,v) <- removeFrom){
      if(k<cutoff){
        removed +=1
        removeFrom.remove(k)

      }
    }

    for ((propkey, propval) <- properties) {
      propRemoval = propRemoval + propval.removeAncientHistory(cutoff,compressing)
    } //do the same for all properties
    val allOld = newestPoint.get<cutoff && !head //all points older than cutoff and latest update is deletion
    (false,allOld,removed,propRemoval)
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

  def beenSaved():Boolean=saved
  def firstSave():Unit = saved =true

  def latestRouterCheck(newRouter:Int):Boolean = newRouter==latestRouter
  def updateLatestRouter(newRouter:Int):Unit = latestRouter = newRouter

  def wipe() = {
    if (addOnly)
      oldestPoint.set(Long.MaxValue)
    else
      previousState = mutable.TreeMap()(HistoryOrdering)
  }

  def getHistorySize() : Int = {
    if (addOnly)
      if(oldestPoint.get == Long.MaxValue)
        0 //is a placeholder entity
      else
        1
    else
      previousState.size + compressedState.size
  }

  def getUncompressedSize() : Int = {
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

//if (!addOnly) {
//  previousState = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
//  compressedState = mutable.TreeMap()(HistoryOrdering)
// }