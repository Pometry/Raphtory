package com.raphtory.algorithms

import com.raphtory.{BaseCorrectnessTest, TestQuery}
import com.raphtory.algorithms.generic.VertexHistogram
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceOrFileSpout

class HistogramTest extends BaseCorrectnessTest {

  test("Histogram Test on in-degree") {
    correctnessTest(
            TestQuery(Degree() -> VertexHistogram[Int]("inDegree", noBins = 5), 23),
            "Histogram/inHistogramResult.csv"
    )
  }

  test("Histogram Test on out-degree") {
    correctnessTest(
            TestQuery(Degree() -> VertexHistogram[Int]("outDegree", noBins = 5), 23),
            "Histogram/outHistogramResult.csv"
    )
  }

  test("Histogram Test on total degree") {
    correctnessTest(
            TestQuery(Degree() -> VertexHistogram[Int]("degree", noBins = 5), 23),
            "Histogram/totalHistogramResult.csv"
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceOrFileSpout("/MotifCount/motiftest.csv"))
}
