package com.raphtory.algorithms

import com.raphtory._
import com.raphtory.algorithms.generic.HITS
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout

class HITSTest extends FPCorrectnessTest(Set(2, 3), tol = 1e-4) {
  test("Test HITS on small graph") {
    correctnessTest(
            TestQuery(HITS(), 1),
            "HITS/outHITS.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("HITS/inHITS.csv"))
}
