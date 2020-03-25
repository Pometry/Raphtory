package com.raphtory.core.model.graphentities

import com.raphtory.core.storage.EntityStorage
import com.raphtory.core.utils.HistoryOrdering
import monix.execution.atomic.AtomicLong

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

abstract class Entity(val creationTime: Long, isInitialValue: Boolean,storage:EntityStorage) {

  // Properties from that entity
  private var entityType:String = null
  var properties:ParTrieMap[String,Property] = ParTrieMap[String, Property]()

  // History of that entity
  var previousState : mutable.TreeMap[Long, Boolean] = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)
  var compressedState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
  private var saved = false
  var shouldBeWiped = false

  var oldestPoint : AtomicLong=  AtomicLong(creationTime)
  var newestPoint: AtomicLong = AtomicLong(creationTime)

  // History of that entity
  var removeList: mutable.TreeMap[Long,Boolean] = mutable.TreeMap()(HistoryOrdering)
  if(!isInitialValue)
    removeList = mutable.TreeMap(creationTime -> isInitialValue)(HistoryOrdering)

  def setType(newType:String): Unit = if(entityType ==(null)) entityType=newType
  def getType:String = if(entityType == null) "" else entityType

  def revive(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
    previousState.put(msgTime, true)
  }

  def kill(msgTime: Long): Unit = {
    checkOldestNewest(msgTime)
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
    */
  def apply(property: String): Property = properties(property)

  def compressHistory(cutoff:Long): mutable.TreeMap[Long, Boolean] ={
    if(previousState.isEmpty) return mutable.TreeMap()(HistoryOrdering) //if we have no need to compress, return an empty list
    if (previousState.takeRight(1).head._1>=cutoff) return mutable.TreeMap()(HistoryOrdering) //if the oldest point is younger than the cut off no need to do anything
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
      if (compressedState.isEmpty || (compressedState.head._2 != PriorPoint._2)) { //if the last point is before the cut off --if compressedState is empty or the head of the compressed state is different to the final point of the uncompressed state then write
        toWrite.put(PriorPoint._1, PriorPoint._2) //add to toWrite so this can be saved to cassandra
        compressedState.put(PriorPoint._1, PriorPoint._2) //add to compressedState in-mem
      }
    }
    else
      newPreviousState.put(PriorPoint._1,PriorPoint._2) //if the last point in the uncompressed history wasn't past the cutoff it needs to go back into the new uncompressed history
    previousState = newPreviousState
    toWrite
  }

  //val removeFrom = if(compressing) compressedState else previousState
  def archive(cutoff:Long, compressing:Boolean,entityType:Boolean,workerID:Int): Boolean={ //
    if(previousState.isEmpty && compressedState.isEmpty) return false //blank node, decide what to do later
    if(compressedState.nonEmpty){
      if (compressedState.takeRight(1).head._1>=cutoff) return false //if the oldest point is younger than the cut off no need to do anything
      var head = compressedState.head._2 //get the head of later
      val newCompressedState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
      compressedState.foreach{case (k,v) => {
        if(k>=cutoff)
          newCompressedState put(k,v)
        else recordRemoval(entityType,workerID)
      }} //for each point if it is safe then keep it
      compressedState = newCompressedState //overwrite the compressed state
      //properties.foreach{case ((propkey, property)) =>{property.archive(cutoff,compressing,entityType)}}//do the same for all properties //currently properties should exist until entity removed
      newestPoint.get<cutoff && !head //return all points older than cutoff and latest update is deletion
    }
    false
  }

  def recordRemoval(entityType:Boolean,workerID:Int) = {
    if(entityType)
      storage.vertexHistoryDeletionCount(workerID)+=1
    else
      storage.edgeHistoryDeletionCount(workerID)+=1
  }

  def archiveOnly(cutoff:Long, entityType:Boolean,workerID:Int): Boolean={ //
    if(previousState.isEmpty) return false //blank node, decide what to do later
    if (previousState.takeRight(1).head._1>=cutoff) return false //if the oldest point is younger than the cut off no need to do anything

    var head = previousState.head._2 //get the head of later
    val newPreviousState: mutable.TreeMap[Long, Boolean] = mutable.TreeMap()(HistoryOrdering)
    previousState.foreach{case (k,v) => {
      if(k>=cutoff)
        newPreviousState put(k,v)
      else
        recordRemoval(entityType,workerID)
    }} //for each point if it is safe then keep it
    previousState = newPreviousState //overwrite the compressed state
      newestPoint.get<cutoff && !head //return all points older than cutoff and latest update is deletion
    false
  }

  /** *
    * Add or update the property from an edge or a vertex based, using the operator vertex + (k,v) to add new properties
    *
    * @param msgTime Message ID where the update came from
    * @param key   property key
    * @param value property value
    */
  def +(msgTime: Long,immutable: Boolean, key: String, value: Any): Unit = {
    properties.get(key) match {
      case Some(p) => p update(msgTime, value)
      case None => if(immutable) properties.put(key, new MutableProperty(msgTime, key, value,storage)) else properties.put(key, new ImmutableProperty(msgTime, key, value,storage))
    }
  }



  def beenSaved():Boolean=saved
  def firstSave():Unit = saved =true


  def wipe() = {previousState = mutable.TreeMap()(HistoryOrdering)}

  def getHistorySize() : Int = {previousState.size + compressedState.size}

  def getUncompressedSize() : Int = {previousState.size}

  def getId : Long
  def getPropertyCurrentValue(key : String) : Option[Any] =
    properties.get(key) match {
      case Some(p) => Some(p.currentValue)
      case None => None
    }


  protected val stateCache = ParTrieMap[Long, Long]()

  protected def closestTime(time:Long):(Long,Boolean)  = {
    var closestTime:Long = -1
    var value = false
    for((k,v) <- previousState)
      if(k<=time)
        if((time-k)<(time-closestTime)) {
          closestTime = k
          value = v
        }
    (closestTime,value)
  }

  def aliveAt(time:Long):Boolean = {
    if(time < oldestPoint.get)
      false
    else {
      val closest = closestTime(time)
      closest._2
    }
  }

  def aliveAtWithWindow(time:Long,windowSize:Long):Boolean = {
    if(time < oldestPoint.get)
      false
    else{
      val closest = closestTime(time)
      if(time-closest._1<=windowSize)
        closest._2
      else false
    }
  }


}



//def latestRouterCheck(newRouter:Int):Boolean = newRouter==latestRouter
//def updateLatestRouter(newRouter:Int):Unit = latestRouter = newRouter