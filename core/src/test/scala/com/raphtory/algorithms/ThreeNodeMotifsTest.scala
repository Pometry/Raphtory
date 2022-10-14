package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.ThreeNodeMotifs
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class ThreeNodeMotifsTest extends BaseCorrectnessTest {
  test("test motif counting") {
    correctnessTest(
            TestQuery(ThreeNodeMotifs, 23),
            "MotifCount/motiftestCorrectResults.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
