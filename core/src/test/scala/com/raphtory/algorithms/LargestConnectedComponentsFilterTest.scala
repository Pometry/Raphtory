package com.raphtory.algorithms

import com.raphtory.algorithms.filters.LargestConnectedComponentFilter
import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.api.input.Source
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class LargestConnectedComponentsFilterTest extends BaseCorrectnessTest {
  withGraph.test("Test largest connected components filter") { graph =>
    correctnessTest(
            TestQuery(LargestConnectedComponentFilter(), 7),
            "ConnectedComponents/filterComponentsResults.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("ConnectedComponents/twoComponents.csv"))
}
