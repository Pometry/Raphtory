package com.raphtory.algorithms.generic

import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective

class EdgeStateTest extends Generic {

  override def apply(graph: GraphPerspective): graph.Graph = graph
    .edgeStep( e => e.setState("nodeIDs",e.src.toString+" "+e.dst.toString))
    .edgeStep(e => println(e.getState("nodeIDs")))

}

object EdgeStateTest {
  def apply() = new EdgeStateTest()
}