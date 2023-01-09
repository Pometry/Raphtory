package com.test.raphtory

import com.raphtory.algorithms.generic.NodeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.table.{KeyPair, Row, Table}
import com.raphtory.api.analysis.visitor.Vertex

object AllNeighbours extends Generic {

  override def tabularise(graph: GraphPerspective): Table =
    graph.explodeSelect { vertex =>
      vertex.edges.map(e => Row(KeyPair("vertexName",vertex.name()),KeyPair("sourceID", e.src), KeyPair("targetID",e.dst)))
    }
}
