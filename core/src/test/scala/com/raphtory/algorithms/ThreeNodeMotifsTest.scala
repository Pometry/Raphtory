package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.generic.motif.ThreeNodeMotifs

class ThreeNodeMotifsTest extends BaseCorrectnessTest {
  test("test motif counting") {
    assert(
            correctnessTest(
                    ThreeNodeMotifs(),
                    "MotifCount/motiftest.csv",
                    "MotifCount/motiftestCorrectResults.csv",
                    23
            )
    )
  }
}
