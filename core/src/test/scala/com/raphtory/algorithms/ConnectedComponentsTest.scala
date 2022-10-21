package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.ConnectedComponents
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class ConnectedComponentsTest extends BaseCorrectnessTest {
  withGraph.test("Test two components") { graph =>
    correctnessTest(
            TestQuery(ConnectedComponents, 7),
            "ConnectedComponents/twoComponentsResults.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("ConnectedComponents/twoComponents.csv"))
}
