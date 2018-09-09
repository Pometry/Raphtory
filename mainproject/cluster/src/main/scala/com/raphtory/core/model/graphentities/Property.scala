package com.raphtory.core.model.graphentities

import com.raphtory.core.utils.HistoryOrdering

import scala.collection.mutable
/** *
  * Node or Vertice Property. Created by Mirate on 10/03/2017.
  *
  * @param creationTime
  * @param key           Property name
  * @param value         Property value
  */
class Property(creationTime: Long,
               key: String,
               value: String) {
  private var saved = false

  // Initialize the TreeMap
  var previousState: mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)

  // add in the initial information
  update(creationTime, value)

  def name = key
  /**
    * update the value of the property
    *
    * @param msgTime
    * @param newValue
    */
  def update(msgTime: Long, newValue: String): Unit = {
    previousState.put(msgTime, newValue)
  }

  def compressAndReturnOldHistory(cutoff:Long): mutable.TreeMap[Long, String] ={
    if(getPreviousStateSize==0){ //if the state size is 0 it is a wiped node and should not be interacted with
      return  mutable.TreeMap()(HistoryOrdering) //if the size is one, no need to compress
    }
    saved=true
    var safeHistory : mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)
    var oldHistory : mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)

    val head = previousState.head
    safeHistory += head // always keep at least one point in history

    var prev: (Long,String) = head
    var swapped = false
    for((k,v) <- previousState){
      if(k<cutoff) {
        if(swapped) //as we are adding prev skip the value on the pivot as it is already added
          if (!(v equals prev._2)) {
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

  def getPreviousStateSize() : Int = {
      previousState.size
  }

  override def equals(obj: scala.Any): Boolean = {
    if(obj.isInstanceOf[Property]){
      val prop2 = obj.asInstanceOf[Property]
      if(name.equals(prop2.name) && (previousState.equals(prop2.previousState))){
        return true
      }
      return false
    }
    false
  }

//  def compressAndReturnOldHistory(cutoff:Long): mutable.TreeMap[Long, String] ={
//    var safeHistory : mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)
//    var oldHistory : mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)
//    safeHistory += previousState.head // always keep at least one point in history
//    for((k,v) <- previousState){
//      if(k<cutOff)
//        oldHistory += k ->v
//      else
//        safeHistory += k -> v
//    }
//    (safeHistory,oldHistory)
//    oldHistory
//  }




  /** *
    * returns a string with all the history of that property
    *
    * @return
    */
  override def toString: String = {
    s"Property Key: $name \n History: $previousState"
   // var toReturn = System.lineSeparator()
   // previousState.foreach(p =>
   //   toReturn = s"$toReturn           MessageID ${p._1}: ${p._2} -- ${p._2} " + System
   //     .lineSeparator())
   // s"Property: ${key} ----- Previous State: $toReturn"
  }

  /** *
    * returns a string with only the current value of the property
    *
    * @return
    */
  def toStringCurrent: String = {
    val toReturn = System.lineSeparator() +
      s"           MessageID ${previousState.head._1}: ${previousState.head._2} -- ${previousState.head._2} " +
      System.lineSeparator()
    s"Property: ${key} ----- Current State: $toReturn"
  }

  def currentValue : String = previousState.head._2
  def currentTime : Long    = previousState.head._1
  def beenSaved():Boolean=saved

}
