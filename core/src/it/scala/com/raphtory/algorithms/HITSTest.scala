package com.raphtory.algorithms

import com.raphtory.FPCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.HITS
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class HITSTest extends FPCorrectnessTest(Set(2, 3), tol = 1e-4) {
  test("Test HITS on small graph") {
    correctnessTest(
            TestQuery(HITS(), 1),
            "HITS/outHITS.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("HITS/inHITS.csv"))
}
