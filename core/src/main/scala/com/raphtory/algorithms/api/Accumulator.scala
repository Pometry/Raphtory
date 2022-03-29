package com.raphtory.algorithms.api

/**
  *  {s}`Accumulator[T]`
  *    : Public accumulator interface
  *
  *  ## Methods
  *
  *    {s}`+= (newValue: T): Unit`
  *      : add new value to accumulator
  *
  *    {s}`value: T`
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
