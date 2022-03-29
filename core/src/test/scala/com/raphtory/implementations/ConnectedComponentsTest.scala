package com.raphtory.implementations

import com.raphtory.algorithms.generic.ConnectedComponents

class ConnectedComponentsTest extends BaseCorrectnessTest {
  test("Test two components") {
    assert(
            correctnessTest(
                    ConnectedComponents(),
                    "ConnectedComponents/twoComponents.csv",
                    "ConnectedComponents/twoComponentsResults.csv",
                    7
            )
    )
  }

}
