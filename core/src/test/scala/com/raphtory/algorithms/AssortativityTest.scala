package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.centrality.Assortativity

class AssortativityTest extends BaseCorrectnessTest {
  test("Assortativity"){
    correctnessTest(
      TestQuery(Assortativity(), 1),
      "Assortativity/AssortativityTest.csv",
      "Assortativity/AssortativityResult.csv")
  }
}
