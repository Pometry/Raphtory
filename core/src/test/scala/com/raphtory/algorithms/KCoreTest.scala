package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.KCore

class KCoreTest extends BaseCorrectnessTest {
  test("Test with K-Core = 3") {
    correctnessTest(
      TestQuery(KCore(3), 17),
      "KCore/kCore3.csv",
      "KCore/kCore3Results.csv"
    )
  }

}
