package com.test.raphtory

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex

object AllNeighbours extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph
      .step { vertex =>
        val edges = vertex.edges.toSeq
        vertex.setState("sourceID", edges.map(_.src))
        vertex.setState("targetID", edges.map(_.dst))
      }
      .select("name", "sourceID", "targetID")
      .explode("sourceID", "targetID")
}
