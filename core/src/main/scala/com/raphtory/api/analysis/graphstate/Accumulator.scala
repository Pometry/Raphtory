package com.raphtory.api.analysis.graphstate

/** Accumulator interface.
  *
  * @tparam S Input type for the accumulator
  *
  * @tparam T value type of the accumulator
  *
  * @see [[GraphState]]
  */
trait Accumulator[-S, T] {

  /** Get last accumulated value */
  def value: T

  /** Add new value to accumulator
    * @param newValue Value to add
    */
  def +=(newValue: S): Unit
}
