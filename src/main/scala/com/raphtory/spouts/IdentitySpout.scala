package com.raphtory.spouts

import com.raphtory.core.components.spout.Spout

case class IdentitySpout[T]() extends Spout[T] {
  override def hasNext(): Boolean = false

  override def next(): Option[T] = None

  override def spoutReschedules(): Boolean = false
}
