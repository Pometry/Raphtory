package com.raphtory.api.graphstate

import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.util.Bounded
import com.raphtory.util.Bounded._
import com.raphtory.util.ExtendedNumeric._

/**
  * Public interface for global accumulators
  *
  *  The GraphState tracks global (graph-level) variables during algorithm execution.
  *  Graph-level state takes the form of accumulators which expose the value computed during the last step/iteration
  *  and allow accumulation of new state based on a reduction function.
  *
  * @see [[GraphPerspective]], [[Accumulator]]
  */
abstract class GraphState {

  /**  Create a new general Accumulator with value type `T` under name `name`
    *
    * @param name Reference name of accumulator
    * @param initialValue Initial value for accumulator
    * @param retainState If retainState = true, accumulation for the next step/iteration of an algorithm continues with the
    *                    previously computed value, otherwise, the value is reset to initialValue before each step.
    *
    * @param op Reduction function for the accumulator.
    */
  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit

  /** Create a new constant that stores an immutable value */
  def newConstant[T](
      name: String,
      value: T
  ): Unit

  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String): Unit =
    newAdder[T](name, 0, retainState = false)

  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, initialValue: T): Unit =
    newAdder[T](name, initialValue, retainState = false)

  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, retainState: Boolean): Unit =
    newAdder[T](name, 0, retainState)

  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String): Unit =
    newMultiplier[T](name, 1, retainState = false)

  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String, initialValue: T): Unit =
    newMultiplier[T](name, initialValue, retainState = false)

  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String, retainState: Boolean): Unit =
    newMultiplier[T](name, 1, retainState)

  /** Create a new accumulator that multiplies values */
  def newMultiplier[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T: Numeric: Bounded](
      name: String
  ): Unit = newMax[T](name, MIN[T], retainState = false)

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T: Numeric: Bounded](name: String, initialValue: T): Unit =
    newMax[T](name, initialValue, retainState = false)

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T: Numeric: Bounded](name: String, retainState: Boolean): Unit =
    newMax[T](name, MIN[T], retainState)

  /** Create a new accumulator that tracks the maximum value */
  def newMax[T: Numeric: Bounded](
      name: String,
      initialValue: T,
      retainState: Boolean
  ): Unit

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T: Numeric: Bounded](name: String): Unit =
    newMin(name, MAX[T], retainState = false)

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T: Numeric: Bounded](name: String, initialValue: T): Unit =
    newMin(name, initialValue, retainState = false)

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T: Numeric: Bounded](name: String, retainState: Boolean): Unit =
    newMin(name, MAX[T], retainState)

  /** Create a new accumulator that tracks the minimum value */
  def newMin[T: Numeric: Bounded](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new histogram that tracks the distribution of a graph quantity */
  def newHistogram[T: Numeric](
      name: String,
      noBins: Int,
      minValue: T,
      maxValue: T,
      retainState: Boolean = true
  ): Unit

  /** Create new Boolean accumulator that returns `true` if all accumulated values are `true` and `false` otherwise
    * @param name Name for the accumulator
    * @param retainState if `true` do not reset the accumulated value after each iteration
    */
  def newAll(name: String, retainState: Boolean = false): Unit

  /** Create new Boolean accumulator that returns `true` if any accumulated value is `true` and `false` otherwise */
  def newAny(name: String, retainState: Boolean = false): Unit

  /** Return the accumulator stored under name `name` */
  def apply[S, T](name: String): Accumulator[S, T]

  /** Return the accumulator stored under `name` name if it exists, else return `None` */
  def get[S, T](name: String): Option[Accumulator[S, T]]

  /** Check if graph state with `name` exists */
  def contains(name: String): Boolean
}
