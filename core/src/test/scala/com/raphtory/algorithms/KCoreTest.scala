package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.KCore
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class KCoreTest extends BaseCorrectnessTest {
  test("Test with K-Core = 3") {
    correctnessTest(
            TestQuery(KCore(3), 17),
            "KCore/kCore3Results.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("KCore/kCore3.csv"))
}
