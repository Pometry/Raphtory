package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.distance.ShortestPathDistance
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout
import munit.IgnoreSuite

class ShortestPathDistanceTest extends BaseCorrectnessTest {
  test("Test shortest path distances") {
    correctnessTest(
            TestQuery(new ShortestPathDistance[Int]("1", "8", 100), 23),
            "ShortestPathDistance/shortestpathdistancetestCorrectResults.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("/MotifCount/motiftest.csv"))
}
