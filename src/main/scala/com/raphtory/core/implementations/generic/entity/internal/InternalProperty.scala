package com.raphtory.core.implementations.generic.entity.internal

trait InternalProperty {
  def update(msgTime: Long, newValue: Any): Unit

  def valueAt(time: Long): Any

  def values(): Array[(Long, Any)]

  def valuesAfter(time: Long): Array[Any]

  def currentValue(): Any

  def creation(): Long
}

