package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.centrality.Degree
import com.raphtory.algorithms.generic.centrality.WeightedDegree
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.api.input.Graph
import com.raphtory.api.input.GraphBuilder
import com.raphtory.api.input.Source
import com.raphtory.api.input.Spout
import com.raphtory.spouts.ResourceSpout

class DegreeTest extends BaseCorrectnessTest {

  withGraph.test("weighted Degree with weighted edges") { graph =>
    correctnessTest(TestQuery(WeightedDegree[Long](), 6), "Degree/weightedResult.csv", graph)
  }

  withGraph.test("weighted Degree with edge count") { graph =>
    correctnessTest(
            TestQuery(WeightedDegree[Long](""), 6),
            "Degree/countedResult.csv",
            graph
    )
  }

  withGraph.test("unweighted Degree") { graph =>
    correctnessTest(TestQuery(Degree(), 6), "Degree/unweightedResult.csv", graph)
  }

  override def setSource(): Source = Source(ResourceSpout("Degree/degreeTest.csv"), WeightedGraphBuilder)
}
