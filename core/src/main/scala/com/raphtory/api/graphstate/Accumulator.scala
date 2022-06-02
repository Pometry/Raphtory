package com.raphtory.api.graphstate

/** Abstract class for the Accumulator interface.
  * See also com.raphtory.algorithms.api.GraphState
  */
abstract class Accumulator[-S, T] {

  /** Get last accumulated value */
  def value: T

  /** Add new value to accumulator */
  def +=(newValue: S): Unit
}
