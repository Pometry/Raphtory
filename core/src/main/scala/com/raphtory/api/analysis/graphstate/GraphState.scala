package com.raphtory.api.analysis.graphstate

import com.raphtory.utils.Bounded
import com.raphtory.utils.Bounded._
import com.raphtory.utils.ExtendedNumeric._

/**
  * Public interface for global accumulators
  *
  *  The GraphState tracks global (graph-level) variables during algorithm execution.
  *  Graph-level state takes the form of accumulators which expose the value computed during the last step/iteration
  *  and allow accumulation of new state based on a reduction function.
  *
  * @see [[com.raphtory.api.analysis.graphview.GraphPerspective GraphPerspective]], [[Accumulator]]
  *
  * @define retainState @param retainState If `true`, accumulation for the next step/iteration of an algorithm continues with the
  *                                        previously computed value, otherwise, the value is reset to `initialValue` before each step.
  *
  * @define name @param name Name for the accumulator
  *
  * @define initialValue @param initialValue Initial value for accumulator
  *
  * @define vType @tparam T Value type of the accumulator
  *
  * @define iType @tparam S Input type of the accumulator
  */
abstract class GraphState {

  /**  Create a new general Accumulator
    *
    * $vType
    * $name
    * $initialValue
    * $retainState
    * @param op Reduction function for the accumulator.
    */
  def newAccumulator[T](
      name: String,
      initialValue: T,
      retainState: Boolean = false,
      op: (T, T) => T
  ): Unit

  /** Create a new constant that stores an immutable value
    *
    * @tparam T Value type of the constant
    * $name
    * @param value Value of the constant
    */
  def newConstant[T](
      name: String,
      value: T
  ): Unit

  /** Create a new zero-initialised accumulator that sums values and resets after each step
    *
    * $vType
    * $name
    */
  def newAdder[T: Numeric](name: String): Unit =
    newAdder[T](name, 0, retainState = false)

  /** Create a new accumulator that sums values */
  def newAdder[T: Numeric](name: String, initialValue: T): Unit =
    newAdder[T](name, initialValue, retainState = false)

  /** Create a new zero-initialised accumulator that sums values
    *
    * $vType
    * $name
    * $retainState
    */
  def newAdder[T: Numeric](name: String, retainState: Boolean): Unit =
    newAdder[T](name, 0, retainState)

  /** Create a new accumulator that sums values
    *
    * $vType
    * $name
    * $initialValue
    * $retainState
    */
  def newAdder[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new one-initialised accumulator that multiplies values and resets after each step
    *
    * $vType
    * $name
    */
  def newMultiplier[T: Numeric](name: String): Unit =
    newMultiplier[T](name, 1, retainState = false)

  /** Create a new accumulator that multiplies values and resets after each step
    *
    * $vType
    * $name
    * $initialValue
    */
  def newMultiplier[T: Numeric](name: String, initialValue: T): Unit =
    newMultiplier[T](name, initialValue, retainState = false)

  /** Create a new one-initialised accumulator that multiplies values
    *
    * $vType
    * $name
    * $retainState
    */
  def newMultiplier[T: Numeric](name: String, retainState: Boolean): Unit =
    newMultiplier[T](name, 1, retainState)

  /** Create a new accumulator that multiplies values
    *
    * $vType
    * $name
    * $initialValue
    * $retainState
    */
  def newMultiplier[T: Numeric](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new accumulator that tracks the maximum value and resets after each step
    *
    * $vType
    * $name
    */
  def newMax[T: Numeric: Bounded](
      name: String
  ): Unit = newMax[T](name, MIN[T], retainState = false)

  /** Create a new accumulator that tracks the maximum value and resets after each step
    *
    * $vType
    * $name
    * $initialValue
    */
  def newMax[T: Numeric: Bounded](name: String, initialValue: T): Unit =
    newMax[T](name, initialValue, retainState = false)

  /** Create a new accumulator that tracks the maximum value
    *
    * $vType
    * $name
    * $retainState
    */
  def newMax[T: Numeric: Bounded](name: String, retainState: Boolean): Unit =
    newMax[T](name, MIN[T], retainState)

  /** Create a new accumulator that tracks the maximum value
    *
    * $vType
    * $name
    * $initialValue
    * $retainState
    */
  def newMax[T: Numeric: Bounded](
      name: String,
      initialValue: T,
      retainState: Boolean
  ): Unit

  /** Create a new accumulator that tracks the minimum value and resets after each step
    *
    * $vType
    * $name
    */
  def newMin[T: Numeric: Bounded](name: String): Unit =
    newMin(name, MAX[T], retainState = false)

  /** Create a new accumulator that tracks the minimum value and resets after each step
    *
    * $vType
    * $name
    * $initialValue
    */
  def newMin[T: Numeric: Bounded](name: String, initialValue: T): Unit =
    newMin(name, initialValue, retainState = false)

  /** Create a new accumulator that tracks the minimum value
    *
    * $vType
    * $name
    * $retainState
    */
  def newMin[T: Numeric: Bounded](name: String, retainState: Boolean): Unit =
    newMin(name, MAX[T], retainState)

  /** Create a new accumulator that tracks the minimum value
    *
    * $vType
    * $name
    * $initialValue
    * $retainState
    */
  def newMin[T: Numeric: Bounded](name: String, initialValue: T, retainState: Boolean): Unit

  /** Create a new histogram that tracks the distribution of a graph quantity
    *
    * @param name Name for the histogram
    * @param noBins Number of histogram bins
    * @param minValue Minimum data value for distribution
    * @param maxValue Maximum data value distribution
    * $retainState
    *
    * @tparam T Type of histogram values
    */
  def newHistogram[T: Numeric](
      name: String,
      noBins: Int,
      minValue: T,
      maxValue: T,
      retainState: Boolean = true
  ): Unit

  /** Create a new counter that tracks the counts of a categorical graph quantity
    *
    * @param name Name for the counter
    * $retainState
    *
    * @tparam T Type of counted values
    */
  def newCounter[T](
                                name: String,
                                retainState: Boolean = true
                              ): Unit

  /** Create new Boolean accumulator that returns `true` if all accumulated values are `true` and `false` otherwise
    *
    * $name
    * $retainState
    */
  def newAll(name: String, retainState: Boolean = false): Unit

  /** Create new Boolean accumulator that returns `true` if any accumulated value is `true` and `false` otherwise
    *
    * $name
    * $retainState
    */
  def newAny(name: String, retainState: Boolean = false): Unit

  /** Get the number of nodes in the graph */
  def nodeCount: Int

  /** Retrieve accumulator
    * $iType
    * $vType
    *
    * $name
    *
    * @throws NoSuchElementException if accumulator with `name` does not exist
    * @return The accumulator stored under `name`
    */
  def apply[S, T](name: String): Accumulator[S, T]

  /** Safely retrieve accumulator
    * $iType
    * $vType
    *
    * $name
    *
    * @return The accumulator stored under `name` if it exists, else `None`
    */
  def get[S, T](name: String): Option[Accumulator[S, T]]

  /** Check if accumulator exists
    *
    * $name
    * @return `true` if accumulator with `name` exists else `false`
    */
  def contains(name: String): Boolean
}
