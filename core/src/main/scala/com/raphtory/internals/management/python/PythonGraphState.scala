package com.raphtory.internals.management.python

import com.raphtory.api.analysis.graphstate.Accumulator
import com.raphtory.api.analysis.graphstate.GraphState

class PythonGraphState(gs: GraphState) {

  def set_state_num(key: String, value: Any): Unit =
    gs.apply[Any, Long](key).+=(value)

  def get_state_num(name: String): Accumulator[Any, Long] =
    gs.get[Any, Long](name).orNull

  //  def set_state_double(key: String, value: Any): Unit =
  //    gs.apply[Any, Double](key).+=(value)
  //
  //  def get_state_double(name: String): Accumulator[Any, Double] =
  //    gs.get[Any, Double](name).orNull

  def numAdder(nameProperty: String, initialValue: Long, retainState: Boolean): Unit =
    gs.newAdder(name = nameProperty, initialValue = initialValue, retainState = retainState)

}
