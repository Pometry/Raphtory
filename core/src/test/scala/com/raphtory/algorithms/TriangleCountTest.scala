package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.LocalTriangleCount
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class TriangleCountTest extends BaseCorrectnessTest {
  test("test triangle counting") {
    correctnessTest(
            TestQuery(LocalTriangleCount(), 23),
            "TriangleCount/triangleCorrectResult.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
