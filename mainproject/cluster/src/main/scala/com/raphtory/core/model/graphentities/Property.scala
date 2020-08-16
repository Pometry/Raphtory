package com.raphtory.core.model.graphentities

abstract class Property(creationTime:Long) {
  def update(msgTime: Long, newValue: Any): Unit
  def valueAt(time: Long): Any
  def currentValue(): Any
  def creation():Long = creationTime
}
