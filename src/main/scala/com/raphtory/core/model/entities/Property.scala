package com.raphtory.core.model.entities

import com.raphtory.core.actors.partitionmanager.workers.ParquetProperty

abstract class Property {
  def update(msgTime: Long, newValue: Any): Unit
  def valueAt(time: Long): Any
  def values():Array[(Long,Any)]
  def valuesAfter(time:Long):Array[Any]
  def currentValue(): Any
  def creation():Long
  def serialise(key:String): ParquetProperty
}

