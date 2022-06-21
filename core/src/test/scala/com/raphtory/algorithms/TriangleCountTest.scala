package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.motif.LocalTriangleCount

class TriangleCountTest extends BaseCorrectnessTest {
  test("test triangle counting") {
    correctnessTest(
            TestQuery(LocalTriangleCount, 23),
            "MotifCount/motiftest.csv",
            "TriangleCount/triangleCorrectResult.csv"
    )
  }

}
