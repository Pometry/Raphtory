package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.ThreeNodeMotifs

class ThreeNodeMotifsTest extends BaseCorrectnessTest {
  test("test motif counting") {
    assert(
            correctnessTest(
                    TestQuery(ThreeNodeMotifs, 23),
                    "MotifCount/motiftest.csv",
                    "MotifCount/motiftestCorrectResults.csv"
            )
    )
  }
}
