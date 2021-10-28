package com.raphtory.core.implementations.pojograph.entities.internal

abstract class Property {
  def update(msgTime: Long, newValue: Any): Unit
  def valueAt(time: Long): Any
  def values():Array[(Long,Any)]
  def valuesAfter(time:Long):Array[Any]
  def currentValue(): Any
  def creation():Long
  //def serialise(key:String): ParquetProperty
}

object Property{
//  def apply(parquet: ParquetProperty):Property = {
//    if(parquet.immutable)
//      new ImmutableProperty(parquet.history.head._1,parquet.history.head._2)
//    else {
//      val prop = new MutableProperty(parquet.history.head._1,parquet.history.head._2)
//      prop.previousState ++= parquet.history
//      prop
//    }
//  }
}

