package com.raphtory.core.model.graphentities

import scala.collection.mutable

/** *
  * Node or Vertice Property. Created by Mirate on 10/03/2017.
  *
  * @param creationTime
  * @param value         Property value
  */
class MutableProperty(creationTime: Long, value: Any) extends Property {
  object HistoryOrdering extends Ordering[Long] {
    def compare(key1: Long, key2: Long) = key2.compareTo(key1)
  }
  var previousState: mutable.TreeMap[Long, Any]   = mutable.TreeMap()(HistoryOrdering)
  // add in the initial information
  update(creationTime, value)

  var earliest = creationTime
  var earliestval = value
  override def creation(): Long = earliest

  def update(msgTime: Long, newValue: Any): Unit = {
    if(msgTime<earliest){
      earliest=msgTime
      earliestval=newValue
    }
    previousState.put(msgTime, newValue)
  }

  def valueAt(time: Long): Any = {
    var closestTime: Long = 0
    var value: Any        = earliestval
    for ((k, v) <- previousState)
      if (k <= time)
        if ((time - k) < (time - closestTime)) {
          closestTime = k
          value = v
        }
    value
  }

  override def valuesAfter(time: Long): Array[Any] = {previousState.filter(x=> x._1>=time).map(x=>x._2).toArray}

  def currentValue: Any    = previousState.head._2
  def currentTime: Long    = previousState.head._1

  override def values(): Array[(Long,Any)] =  {previousState.toArray}
}
