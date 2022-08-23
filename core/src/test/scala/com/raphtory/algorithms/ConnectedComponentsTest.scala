package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.ConnectedComponents

class ConnectedComponentsTest extends BaseCorrectnessTest {
  test("Test two components") {
    correctnessTest(
            TestQuery(ConnectedComponents, 7),
            "ConnectedComponents/twoComponents.csv",
            "ConnectedComponents/twoComponentsResults.csv"
    )
  }

}
