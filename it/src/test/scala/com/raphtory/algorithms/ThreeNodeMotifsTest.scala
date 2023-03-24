package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.motif.ThreeNodeMotifs
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout

class ThreeNodeMotifsTest extends BaseCorrectnessTest {
  test("test motif counting") {
    correctnessTest(
            TestQuery(ThreeNodeMotifs, 23),
            "MotifCount/motiftestCorrectResults.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("/MotifCount/motifTestSLoops.csv"))
}
