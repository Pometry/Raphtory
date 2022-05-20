package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.generic.{VertexHistogram}
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.filters.VertexFilter

class HistogramTest extends BaseCorrectnessTest{

  test("Histogram Test on in-degree") {
    assert(
      correctnessTest(
        Degree() -> VertexHistogram[Int]("inDegree",noBins = 5),
        "MotifCount/motiftest.csv",
        "Histogram/inHistogramResult.csv",
        23
      )
    )
  }

  test("Histogram Test on out-degree") {
    assert(
      correctnessTest(
        Degree() -> VertexHistogram[Int]("outDegree",noBins = 5),
        "MotifCount/motiftest.csv",
        "Histogram/outHistogramResult.csv",
        23
      )
    )
  }

  test("Histogram Test on total degree") {
    assert(
      correctnessTest(
        Degree() -> VertexHistogram[Int]("degree",noBins = 5),
        "MotifCount/motiftest.csv",
        "Histogram/totalHistogramResult.csv",
        23
      )
    )
  }

}
