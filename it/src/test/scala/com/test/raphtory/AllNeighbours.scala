package com.test.raphtory

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.Vertex

object AllNeighbours extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph.explodeSelect { vertex =>
      vertex.edges.map(e => Row(vertex.name(), e.src, e.dst))
    }
}

case class ShitVertexFilter(f: Vertex => Boolean) extends Generic with Serializable{

  override def apply(graph: GraphPerspective): graph.Graph =
    graph.vertexFilter(f)
}
