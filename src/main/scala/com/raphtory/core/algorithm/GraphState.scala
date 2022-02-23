package com.raphtory.core.algorithm

import scala.collection.mutable
import scala.reflect.runtime.universe._

abstract class Accumulator[T](initialValue: T, retainState: Boolean = false, op: (T, T) => T) {
  var value: T = initialValue

  def +=(newValue: T): Unit
}

abstract class GraphState {

  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit

  def newAdder[T: Numeric](name: String): Unit
  def newAdder[T: Numeric](name: String, initialValue: T): Unit
  def newAdder[T: Numeric](name: String, retainState: Boolean): Unit
  def newAdder[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  def apply[T: TypeTag](name: String): Accumulator[T]
}
