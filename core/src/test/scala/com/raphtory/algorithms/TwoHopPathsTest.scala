package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.TwoHopPaths

class TwoHopPathsTest extends BaseCorrectnessTest {
  test("Test two-hop Paths") {
    correctnessTest(
            TestQuery(TwoHopPaths("1", "7"), 23),
            "MotifCount/motiftest.csv",
            "TwoHopPaths/twohopResults.csv"
    )
  }

}
