package com.raphtory.storage.pojograph.entities.internal

import scala.collection.mutable
import com.raphtory.util.OrderedBuffer._

import scala.collection.Searching.Found
import scala.collection.Searching.InsertionPoint

/**
  * @param creationTime
  * @param value         Property value
  */
class MutableProperty(creationTime: Long, value: Any) extends Property {
  var previousState: mutable.ArrayBuffer[(Long, Any)] = mutable.ArrayBuffer()
  // add in the initial information
  update(creationTime, value)

  var earliest: Long            = creationTime
  var earliestval: Any          = value
  override def creation(): Long = earliest

  def update(msgTime: Long, newValue: Any): Unit = {
    if (msgTime < earliest) {
      earliest = msgTime
      earliestval = newValue
    }
    previousState.sortedAppend(msgTime, newValue)
  }

  def valueAt(time: Long): Option[Any] =
    if (time < earliest)
      None
    else {
      val index = previousState.search((time, None))(TupleByFirstOrdering)
      index match {
        case Found(i)          => Some(previousState(i)._2)
        case InsertionPoint(i) => Some(previousState(i - 1)._2)
      }
    }

  override def valueHistory(
      after: Long = Long.MinValue,
      before: Long = Long.MaxValue
  ): Array[(Long, Any)] =
    previousState.filter(x => (x._1 >= after) && (x._1 <= before)).toArray

  def currentValue(): Any = previousState.head._2
  def currentTime(): Long = previousState.head._1

  override def values(): Array[(Long, Any)] = previousState.toArray

  //def serialise(key:String):ParquetProperty = ParquetProperty(key,false,previousState.map(x=>(x._1,x._2.toString)).toList)

}
