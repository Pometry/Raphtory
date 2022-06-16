package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.dynamic.WattsCascade

import scala.::

class WattsTest extends BaseCorrectnessTest {
  test("Test Watts cascade on small example") {
    correctnessTest(
            TestQuery(WattsCascade(threshold = 0.5, infectedSeed = Set("1")), 23),
            "MotifCount/motiftest.csv",
            "WattsCascade/wattsResults.csv"
    )
  }
}
