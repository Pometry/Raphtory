package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.TriangleCount

class TriangleCountTest extends BaseCorrectnessTest {
  test("test triangle counting") {
    assert(
            correctnessTest(
                    TestQuery(TriangleCount, 23),
                    "MotifCount/motiftest.csv",
                    "TriangleCount/triangleCorrectResult.csv"
            )
    )
  }

}
