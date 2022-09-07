package com.raphtory.algorithms

import com.raphtory.algorithms.generic.centrality.PageRank
import com.raphtory.FPCorrectnessTest
import com.raphtory.TestQuery

class PageRankTest extends FPCorrectnessTest(tol = 1e-4) {
  test("test PageRank") {
    correctnessTest(TestQuery(PageRank(0.85, 1000), 23), "MotifCount/motiftest.csv", "PageRank/pagerankresult.csv")
  }

}
