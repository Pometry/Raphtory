package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.TwoHopPaths
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class TwoHopPathsTest extends BaseCorrectnessTest {
  withGraph.test("Test two-hop Paths") { graph =>
    correctnessTest(
            TestQuery(TwoHopPaths("1", "7"), 23),
            "TwoHopPaths/twohopResults.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
