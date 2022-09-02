package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.Coreness

class CorenessTest extends BaseCorrectnessTest {
  test("Coreness Test, start = 1, end = 5") {
    correctnessTest(
      TestQuery(Coreness(1, 5), 29),
      "KCore/CorenessInput.csv",
      "KCore/CorenessResults.csv"
    )
  }

}
