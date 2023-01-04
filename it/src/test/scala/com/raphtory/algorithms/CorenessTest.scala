package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.Coreness
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout

class CorenessTest extends BaseCorrectnessTest {
  test("Coreness Test, start = 1, end = 5") {
    correctnessTest(
            TestQuery(Coreness(1, 5), 29),
            "KCore/CorenessResults.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("/KCore/CorenessInput.csv"))
}
