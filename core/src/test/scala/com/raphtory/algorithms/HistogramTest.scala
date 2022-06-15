package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.filters.VertexFilter
import com.raphtory.algorithms.generic.VertexHistogram
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input.Spout
import com.raphtory.spouts.ResourceSpout

class HistogramTest extends BaseCorrectnessTest(startGraph = true) {
  override def setSpout(): Spout[String] = ResourceSpout("MotifCount/motiftest.csv")

  test("Histogram Test on in-degree") {
    correctnessTest(
            TestQuery(Degree -> VertexHistogram[Int]("inDegree", noBins = 5), 23),
            "Histogram/inHistogramResult.csv"
    )
  }

  test("Histogram Test on out-degree") {
    correctnessTest(
            TestQuery(Degree -> VertexHistogram[Int]("outDegree", noBins = 5), 23),
            "Histogram/outHistogramResult.csv"
    )
  }

  test("Histogram Test on total degree") {
    correctnessTest(
            TestQuery(Degree -> VertexHistogram[Int]("degree", noBins = 5), 23),
            "Histogram/totalHistogramResult.csv"
    )
  }

}
