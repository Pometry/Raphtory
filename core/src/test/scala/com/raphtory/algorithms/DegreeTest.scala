package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.api.input.{Graph, GraphBuilder, Source, Spout}
import com.raphtory.spouts.ResourceSpout

class DegreeTest extends BaseCorrectnessTest {


  test("weighted Degree with weighted edges") {
    correctnessTest(TestQuery(WeightedDegree[Long](), 6), "Degree/weightedResult.csv")
  }

  test("weighted Degree with edge count") {
    correctnessTest(
            TestQuery(WeightedDegree[Long](""), 6),
            "Degree/countedResult.csv"
    )
  }

  test("unweighted Degree") {
    correctnessTest(TestQuery(Degree(), 6), "Degree/unweightedResult.csv")
  }

  override def setSource(): Source = Source(ResourceSpout("Degree/degreeTest.csv"), WeightedGraphBuilder)
}
