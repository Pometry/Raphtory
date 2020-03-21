package com.raphtory.core.model.graphentities

trait Property {
  def update(msgTime: Long, newValue: Any): Unit
  def valueAt(time:Long):Any
  def currentValue():Any
}
