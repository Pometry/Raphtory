package com.raphtory.internals.management.python

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.algorithms.generic.TwoHopPaths
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.algorithms.generic.motif.LocalTriangleCount
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.internals.management.PythonInterop
import com.raphtory.sinks.FileSink
import com.raphtory.sinks.LocalQueueSink

import scala.collection.JavaConverters._

class PythonEntrypoint(graph: GraphPerspective) {

  def raphtoryGraph(): GraphPerspective = graph

  def interop = PythonInterop
}
