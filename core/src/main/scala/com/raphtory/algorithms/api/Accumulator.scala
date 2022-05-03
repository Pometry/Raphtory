package com.raphtory.algorithms.api

/**
  *  `Accumulator[T]`
  *    : Public accumulator interface
  *
  *  ## Methods
  *
  *    `+= (newValue: T): Unit`
  *      : add new value to accumulator
  *
  *    `value: T`
  *      : get last accumulated value
  *
  * ```{seealso}
  * [](com.raphtory.algorithms.api.GraphState)
  * ```
  */
abstract class Accumulator[T] {
  def value: T

  def +=(newValue: T): Unit
}
