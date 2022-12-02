package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.dynamic.WattsCascade
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.{ResourceOrFileSpout, ResourceSpout}

import scala.::

class WattsTest extends BaseCorrectnessTest {
  test("Test Watts cascade on small example") {
    correctnessTest(
            TestQuery(WattsCascade(threshold = 0.5, infectedSeed = Set("1")), 23),
            "WattsCascade/wattsResults.csv"
    )
  }
  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("MotifCount/motiftest.csv"))
}
