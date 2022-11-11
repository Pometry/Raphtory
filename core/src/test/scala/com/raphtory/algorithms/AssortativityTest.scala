package com.raphtory.algorithms

import com.raphtory.FPCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.centrality.Assortativity

class AssortativityTest extends FPCorrectnessTest {
  test("Assortativity"){
    correctnessTest(
      TestQuery(Assortativity(), 1),
      "Assortativity/AssortativityTest.csv",
      "Assortativity/AssortativityResult.csv")
  }
}
