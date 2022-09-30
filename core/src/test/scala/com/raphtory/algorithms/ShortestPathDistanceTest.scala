package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.distance.ShortestPathDistance

class ShortestPathDistanceTest extends BaseCorrectnessTest {
  test("Test shortest path distances") {
    correctnessTest(
      TestQuery(new ShortestPathDistance[Int]("1", "8", 100), 23),
      "MotifCount/motiftest.csv",
      "ShortestPathDistance/shortestpathdistancetestCorrectResults.csv"
    )
  }

}