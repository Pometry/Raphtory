package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.distance.ShortestPathDistance
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class ShortestPathDistanceTest extends BaseCorrectnessTest {
  withGraph.test("Test shortest path distances") { graph =>
    correctnessTest(
            TestQuery(new ShortestPathDistance[Int]("1", "8", 100), 23),
            "ShortestPathDistance/shortestpathdistancetestCorrectResults.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
