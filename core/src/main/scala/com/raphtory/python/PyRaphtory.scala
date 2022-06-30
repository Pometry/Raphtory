package com.raphtory.python

import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.analysis.graphview.DeployedTemporalGraph
import com.raphtory.sinks.FileSink

class PyRaphtory(graph: DeployedTemporalGraph) {

  def connected_components(): ConnectedComponents =
    new ConnectedComponents()

  def file_sink(path: String): FileSink =
    FileSink(filePath = path)

  def raphtory_graph(): DeployedTemporalGraph = graph

}
