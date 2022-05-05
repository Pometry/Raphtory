package com.raphtory.algorithms.api

/** Abstract class for the Accumulator interface.
  * See also com.raphtory.algorithms.api.GraphState */
abstract class Accumulator[T] {

  /** Add new value to accumulator */
  def value: T

  /** Get last accumulated value */
  def +=(newValue: T): Unit
}
