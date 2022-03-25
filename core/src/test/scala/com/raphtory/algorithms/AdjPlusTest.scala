package com.raphtory.algorithms

import com.raphtory.algorithms.generic.AdjPlus

class AdjPlusTest extends BaseCorrectnessTest {
  test("Test AdjPlus projection on small example") {
    assert(correctnessTest(AdjPlus(), "MotifCount/motiftest.csv", "AdjPlus/adjPlusResults.csv", 23))
  }
}
