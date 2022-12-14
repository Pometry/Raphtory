package com.raphtory.algorithms

import com.raphtory.api.input.Source
import com.raphtory.spouts.{ResourceOrFileSpout, ResourceSpout}
import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.temporal.motif.{LocalThreeNodeMotifs, ThreeNodeMotifs}
import com.raphtory.sources.CSVEdgeListSource

class ThreeNodesTMotifTest extends BaseCorrectnessTest {
  test("test temporal motif counting") {
    correctnessTest(
            TestQuery(ThreeNodeMotifs(graphWide = true, prettyPrint = false, delta = 10), 23),
            "MotifCount/tMotifCorrectResults.csv"
    )
  }
  test("test local temporal motif counting") {
    correctnessTest(
            TestQuery(LocalThreeNodeMotifs(prettyPrint = false, delta = 10), 23),
            "MotifCount/tMotifLocalResults.csv"
    )
  }

  def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("/MotifCount/motiftest.csv"))
}
