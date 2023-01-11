package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout

class ConnectedComponentsTest extends BaseCorrectnessTest {
  test("Test two components") {
    correctnessTest(
            TestQuery(ConnectedComponents, 7),
            "ConnectedComponents/twoComponentsResults.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("/ConnectedComponents/twoComponents.csv"))
}
