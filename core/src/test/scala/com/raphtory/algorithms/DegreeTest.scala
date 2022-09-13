package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.api.input.Graph
import com.raphtory.api.input.Spout
import com.raphtory.internals.graph.GraphBuilder
import com.raphtory.spouts.ResourceSpout

class DegreeTest extends BaseCorrectnessTest {
  override def setGraphBuilder(): (Graph, String) => Unit = WeightedGraphBuilder.parse

  override def setSpout(): Spout[String] = ResourceSpout("Degree/degreeTest.csv")

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
}
