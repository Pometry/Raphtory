package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.filters.VertexFilter
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.Vertex

class VertexFilterTest extends BaseCorrectnessTest {

  test("Vertex is being filtered") {
    correctnessTest(
            TestQuery(VertexFilter(_.ID != 1) -> EdgeList(), 23),
            "MotifCount/motiftest.csv",
            "VertexFilter/motifFilterTest.csv"
    )
  }
}
