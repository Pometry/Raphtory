package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.filters.VertexFilter
import com.raphtory.algorithms.generic.VertexHistogram
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.spouts.ResourceSpout

class HistogramTest extends BaseCorrectnessTest {

  withGraph.test("Histogram Test on in-degree") { graph =>
    correctnessTest(
            TestQuery(Degree() -> VertexHistogram[Int]("inDegree", noBins = 5), 23),
            "Histogram/inHistogramResult.csv",
            graph
    )
  }

  withGraph.test("Histogram Test on out-degree") { graph =>
    correctnessTest(
            TestQuery(Degree() -> VertexHistogram[Int]("outDegree", noBins = 5), 23),
            "Histogram/outHistogramResult.csv",
            graph
    )
  }

  withGraph.test("Histogram Test on total degree") { graph =>
    correctnessTest(
            TestQuery(Degree() -> VertexHistogram[Int]("degree", noBins = 5), 23),
            "Histogram/totalHistogramResult.csv",
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
