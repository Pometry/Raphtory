package com.raphtory.algorithms

import com.raphtory.algorithms.filters.LargestConnectedComponentFilter
import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.ConnectedComponents

class LargestConnectedComponentsFilterTest extends BaseCorrectnessTest {
  test("Test largest connected components filter") {
    correctnessTest(
      TestQuery(LargestConnectedComponentFilter(), 7),
      "ConnectedComponents/twoComponents.csv",
      "ConnectedComponents/filterComponentsResults.csv"
    )
  }
}
