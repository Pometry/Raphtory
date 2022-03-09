package com.raphtory.components.spout

import com.raphtory.components.graphbuilder.GraphBuilder

import scala.reflect.runtime.universe._

trait Spout[T] extends Iterator[T] {
  def hasNextIterator(): Boolean  = hasNext
  def nextIterator(): Iterator[T] = this

  def close(): Unit = {}

  def spoutReschedules(): Boolean
  def executeReschedule(): Unit = {}
}
