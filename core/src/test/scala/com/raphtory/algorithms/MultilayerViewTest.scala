package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.algorithms.temporal.TemporalNodeList
import com.raphtory.algorithms.temporal.views.MultilayerView
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy
import com.raphtory.api.input.sources.CSVEdgeListSource
import com.raphtory.api.input.Source
import com.raphtory.spouts.SequenceSpout

class WriteValue extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph =
    graph.multilayerView
      .step(vertex => vertex.setState("testing", 1))
      .reducedView(PropertyMergeStrategy.sum[Int])

}

object WriteValue {
  def apply() = new WriteValue
}

class MultilayerViewTest extends BaseCorrectnessTest {
  val edges: Seq[String] = Seq("1,2,1", "2,1,2")

  withGraph.test("test multilayer view") { graph =>
    correctnessTest(
            TestQuery(MultilayerView() -> EdgeList(), 2),
            Seq("2,1_1,2_1", "2,2_2,1_2"),
            graph
    )
  }

  withGraph.test("test temporal node list") { graph =>
    correctnessTest(
            TestQuery(TemporalNodeList(), 2),
            Seq[String]("2,1,1", "2,1,2", "2,2,1", "2,2,2"),
            graph
    )
  }

  withGraph.test("test temporal edge list") { graph =>
    correctnessTest(TestQuery(TemporalEdgeList(), 2), Seq("2,1,2,1", "2,2,1,2"), graph)
  }

  withGraph.test("test property merging") { graph =>
    correctnessTest(
            TestQuery(WriteValue() -> NodeList("testing"), 2),
            Seq("2,1,2", "2,2,2"),
            graph
    )
  }

  override def setSource(): Source = CSVEdgeListSource(SequenceSpout(edges))
}
