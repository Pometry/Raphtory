package com.raphtory.core.components.spout

trait Spout[T] {

  def hasNext(): Boolean

  def next(): Option[T]

  def close(): Unit = {}
  def reschedule(): Boolean

}
