package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.AdjPlus
import com.raphtory.api.input.Spout
import com.raphtory.spouts.ResourceSpout

class AdjPlusTest extends BaseCorrectnessTest(startGraph = true) {
  override def setSpout(): Spout[String] = ResourceSpout("MotifCount/motiftest.csv")
  withGraph.test("Test AdjPlus projection on small example") {
    correctnessTest(TestQuery(AdjPlus, 23), "AdjPlus/adjPlusResults.csv")
  }
}
