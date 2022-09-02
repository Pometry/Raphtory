package com.raphtory.algorithms

import com.raphtory.algorithms.temporal.motif.{LocalThreeNodeMotifs, ThreeNodeMotifs}
import com.raphtory.{BaseCorrectnessTest, TestQuery}

class ThreeNodesTMotifTest extends BaseCorrectnessTest {
  test("test temporal motif counting") {
    correctnessTest(
      TestQuery(ThreeNodeMotifs(graphWide = true, prettyPrint = false,delta = 10),23),
      "MotifCount/motiftest.csv",
      "MotifCount/tMotifCorrectResults.csv"
    )
  }
  test("test local temporal motif counting") {
    correctnessTest(
      TestQuery(LocalThreeNodeMotifs(prettyPrint = false,delta = 10),23),
      "MotifCount/motiftest.csv",
      "MotifCount/tMotifLocalResults.csv"
    )
  }
}
