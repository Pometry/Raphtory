package com.raphtory.internals.management.python

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.sinks.FileSink
import com.raphtory.sinks.LocalQueueSink

import java.util.concurrent.ArrayBlockingQueue

class PythonEntrypoint(graph: GraphPerspective) {

  def raphtoryGraph(): GraphPerspective = graph

  def connectedComponents = new ConnectedComponents

  def fileSink(path: String): FileSink = FileSink(path)

  def newSyncQueue = new ArrayBlockingQueue[String](Int.MaxValue)

  def localSink(queue: ArrayBlockingQueue[String]) =
    new LocalQueueSink(queue)
}
