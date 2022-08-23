package com.raphtory.internals.management.python

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.management.PythonInterop

class PythonEntrypoint(graph: GraphPerspective) {

  def raphtoryGraph(): GraphPerspective = graph

  def interop: PythonInterop.type = PythonInterop
}
