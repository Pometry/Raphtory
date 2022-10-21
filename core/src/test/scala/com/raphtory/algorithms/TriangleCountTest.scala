package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.LocalTriangleCount
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class TriangleCountTest extends BaseCorrectnessTest {
  withGraph.test("test triangle counting") { graph =>
    correctnessTest(
            TestQuery(LocalTriangleCount(), 23),
            "TriangleCount/triangleCorrectResult.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
