package com.raphtory.internals.management.python

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.sinks.FileSink
import com.raphtory.sinks.LocalQueueSink

class PythonEntrypoint(graph: GraphPerspective) {

  def raphtoryGraph(): GraphPerspective = graph

  def connectedComponents = new ConnectedComponents

  def fileSink(path: String): FileSink = FileSink(path)

  def localSink(): LocalQueueSink =
    new LocalQueueSink()
}
