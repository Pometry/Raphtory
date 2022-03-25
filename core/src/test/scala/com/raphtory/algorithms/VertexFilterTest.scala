package com.raphtory.algorithms

import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.graph.visitor.Vertex

class VertexFilter extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
    graph
      .filter { (vertex: Vertex) =>
        println(vertex.getProperty("name").get)
        vertex.getProperty("name").get == 3
      }
}

object VertexFilter {
  def apply() = new VertexFilter()
}

class VertexFilterTest extends BaseCorrectnessTest {

  test("Vertex is being filtered") {
    assert(
            correctnessTest(
                    VertexFilter() -> EdgeList(),
                    "MotifCount/motiftest.csv",
                    "MotifCount/motifFilterTest.csv",
                    23
            )
    )
  }
}
