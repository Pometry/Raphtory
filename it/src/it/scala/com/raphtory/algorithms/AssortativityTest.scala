package com.raphtory.algorithms

import com.raphtory._
import com.raphtory.algorithms.generic.centrality.Assortativity
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout

class AssortativityTest extends FPCorrectnessTest(Set(0, 1), tol = 1e-4) {
  test("Assortativity") {
    correctnessTest(TestQuery(Assortativity(), 1), "Assortativity/AssortativityResult.csv")
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("Assortativity/AssortativityTest.csv"))
}
