package com.raphtory.implementations

import com.raphtory.algorithms.generic.TwoHopPaths

class TwoHopPathsTest extends BaseCorrectnessTest {
  test("Test two-hop Paths") {
    assert(
            correctnessTest(
                    TwoHopPaths("1", "7"),
                    "MotifCount/motiftest.csv",
                    "TwoHopPaths/twohopResults.csv",
                    23
            )
    )
  }

}
