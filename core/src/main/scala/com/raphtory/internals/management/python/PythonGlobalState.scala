package com.raphtory.internals.management.python

import com.raphtory.api.analysis.graphstate.GraphState

class PythonGlobalState(gs: GraphState) {

  def numAdder(nameProperty: String, initialValue: Long, retainState: Boolean): Unit =
    gs.newAdder(name = nameProperty, initialValue = initialValue, retainState = retainState)

}
