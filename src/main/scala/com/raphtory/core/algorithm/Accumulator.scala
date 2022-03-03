package com.raphtory.core.algorithm

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
  * [](com.raphtory.core.algorithm.GraphState)
  * ```
  */
abstract class Accumulator[T] {
  def value: T

  def +=(newValue: T): Unit
}
