package com.raphtory.algorithms

import com.raphtory.FPCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.centrality.Assortativity
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class AssortativityTest extends FPCorrectnessTest(Set(0,1),tol = 1e-4) {
  test("Assortativity"){
    correctnessTest(
      TestQuery(Assortativity(), 1),
      "Assortativity/AssortativityResult.csv")
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("Assortativity/AssortativityTest.csv"))
}
