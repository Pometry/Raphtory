package com.raphtory.algorithms

import com.raphtory.algorithms.temporal.motif.LocalThreeNodeMotifs
import com.raphtory.algorithms.temporal.motif.ThreeNodeMotifs
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout
import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery

class ThreeNodesTMotifTest extends BaseCorrectnessTest {
  withGraph.test("test temporal motif counting") { graph =>
    correctnessTest(
            TestQuery(ThreeNodeMotifs(graphWide = true, prettyPrint = false, delta = 10), 23),
            "MotifCount/tMotifCorrectResults.csv",
            graph
    )
  }
  withGraph.test("test local temporal motif counting") { graph =>
    correctnessTest(
            TestQuery(LocalThreeNodeMotifs(prettyPrint = false, delta = 10), 23),
            "MotifCount/tMotifLocalResults.csv",
            graph
    )
  }

  def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
