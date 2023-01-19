package com.test.raphtory

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.KeyPair
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex

object AllNeighbours extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph.explodeSelect { vertex =>
      vertex.edges.map(e => Row(("vertexName", vertex.name()), ("sourceID", e.src), ("targetID", e.dst)))
    }
}
