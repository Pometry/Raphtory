package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.filters.VertexFilter
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Vertex

class VertexFilterTest extends BaseCorrectnessTest {

  test("Vertex is being filtered") {
    assert(
            correctnessTest(
                    VertexFilter(_.ID != 1) -> EdgeList(),
                    "MotifCount/motiftest.csv",
                    "VertexFilter/motifFilterTest.csv",
                    23
            )
    )
  }
}
