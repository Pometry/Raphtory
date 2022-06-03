package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.algorithms.temporal.TemporalNodeList
import com.raphtory.algorithms.temporal.views.MultilayerView
import com.raphtory.api.algorithm.Generic
import com.raphtory.api.algorithm.GenericReduction
import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.visitor.PropertyMergeStrategy

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
  val edges = Seq("1,2,1", "2,1,2")

  test("test multilayer view") {
    assert(
            correctnessTest(
                    MultilayerView() -> EdgeList(),
                    edges,
                    Seq("2,1_1,2_1", "2,2_2,1_2"),
                    2
            )
    )
  }

  test("test temporal node list") {
    assert(
            correctnessTest(
                    TemporalNodeList(),
                    edges,
                    Seq("2,1,1", "2,1,2", "2,2,1", "2,2,2"),
                    2
            )
    )
  }

  test("test temporal edge list") {
    assert(
            correctnessTest(TemporalEdgeList(), edges, Seq("2,1,2,1", "2,2,1,2"), 2)
    )
  }

  test("test property merging") {
    assert(
            correctnessTest(WriteValue() -> NodeList("testing"), edges, Seq("2,1,2", "2,2,2"), 2)
    )
  }

}
