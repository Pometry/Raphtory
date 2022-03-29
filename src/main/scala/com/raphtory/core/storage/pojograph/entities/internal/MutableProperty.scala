package com.raphtory.core.storage.pojograph.entities.internal

import scala.collection.mutable

/** *
  * Node or Vertice Property. Created by Mirate on 10/03/2017.
  *
  * @param creationTime
  * @param value         Property value
  */
class MutableProperty(creationTime: Long, value: Any) extends Property {

  var previousState: mutable.ArrayBuffer[(Long, Any)] = mutable.ArrayBuffer()
  // add in the initial information
  update(creationTime, value)

  var earliest                  = creationTime
  var earliestval               = value
  override def creation(): Long = earliest

  def update(msgTime: Long, newValue: Any): Unit = {
    if (msgTime < earliest) {
      earliest = msgTime
      earliestval = newValue
    }
    previousState += ((msgTime, newValue))
  }

  def valueAt(time: Long): Option[Any] =
    if (time < earliest)
      None
    else {
      var closestTime: Long = earliest
      var value: Any        = earliestval
      for ((k, v) <- previousState)
        if (k <= time)
          if ((time - k) < (time - closestTime)) {
            closestTime = k
            value = v
          }
      Some(value)
    }

  override def valuesAfter(time: Long): Array[Any] =
    previousState.filter(x => x._1 >= time).map(x => x._2).toArray

  def currentValue(): Any = previousState.head._2
  def currentTime(): Long = previousState.head._1

  override def values(): Array[(Long, Any)] = previousState.toArray

  //def serialise(key:String):ParquetProperty = ParquetProperty(key,false,previousState.map(x=>(x._1,x._2.toString)).toList)

}
