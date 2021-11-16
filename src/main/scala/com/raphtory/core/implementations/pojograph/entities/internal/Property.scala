package com.raphtory.core.implementations.pojograph.entities.internal

abstract class Property {
  def update(msgTime: Long, newValue: Any): Unit
  def valueAt(time: Long): Any
  def values():Array[(Long,Any)]
  def valuesAfter(time:Long):Array[Any]
  def currentValue(): Any
  def creation():Long
}