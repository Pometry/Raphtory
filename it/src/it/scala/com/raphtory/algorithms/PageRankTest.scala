package com.raphtory.algorithms

import com.raphtory._
import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.api.input.Source
import com.raphtory.sources.CSVEdgeListSource
import com.raphtory.spouts.ResourceSpout

class PageRankTest extends FPCorrectnessTest(tol = 1e-4) {
  test("test PageRank") {
    correctnessTest(TestQuery(PageRank(0.85, 1000), 23), "PageRank/pagerankresult.csv")
  }

  override def setSource(): Source = CSVEdgeListSource(ResourceSpout("MotifCount/motiftest.csv"))
}
