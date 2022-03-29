package com.raphtory.implementations

import com.raphtory.algorithms.generic.motif.TriangleCount

class TriangleCountTest extends BaseCorrectnessTest {
  test("test triangle counting") {
    assert(
            correctnessTest(
                    TriangleCount(),
                    "MotifCount/motiftest.csv",
                    "TriangleCount/triangleCorrectResult.csv",
                    23
            )
    )
  }

}
