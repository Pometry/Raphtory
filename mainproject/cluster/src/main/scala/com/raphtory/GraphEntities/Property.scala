package com.raphtory.GraphEntities

import com.raphtory.utils.HistoryOrdering

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

  // Initialize the TreeMap
  var previousState: mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)

  // add in the initial information
  update(creationTime, value)

  /**
    * update the value of the property
    *
    * @param msgTime
    * @param newValue
    */
  def update(msgTime: Long, newValue: String): Unit = {
    previousState += msgTime -> newValue
  }

  def removeAndReturnOldHistory(cutoff:Long): mutable.TreeMap[Long, String] ={
    val (safeHistory, oldHistory) = historySplit(cutoff)
    previousState = safeHistory
    oldHistory
  }
  def historySplit(cutOff: Long):( mutable.TreeMap[Long, String], mutable.TreeMap[Long, String]) = {

    var safeHistory : mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)
    var oldHistory : mutable.TreeMap[Long, String] = mutable.TreeMap()(HistoryOrdering)

    safeHistory += previousState.head // always keep at least one point in history
    for((k,v) <- previousState){
      if(k<cutOff)
        oldHistory += k ->v
      else
        safeHistory += k -> v
    }
    (safeHistory,oldHistory)
  }




  /** *
    * returns a string with all the history of that property
    *
    * @return
    */
  override def toString: String = {
    var toReturn = System.lineSeparator()
    previousState.foreach(p =>
      toReturn = s"$toReturn           MessageID ${p._1}: ${p._2} -- ${p._2} " + System
        .lineSeparator())
    s"Property: ${key} ----- Previous State: $toReturn"
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

}
