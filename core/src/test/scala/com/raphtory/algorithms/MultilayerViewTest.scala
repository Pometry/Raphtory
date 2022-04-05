package com.raphtory.algorithms

import com.fasterxml.jackson.core.`type`.WritableTypeId
import com.raphtory.algorithms.api.GraphAlgorithm
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.algorithms.temporal.TemporalNodeList
import com.raphtory.algorithms.temporal.views.MultilayerView
import com.raphtory.graph.visitor.PropertyMergeStrategy

class WriteValue extends GraphAlgorithm {

  override def apply(graph: GraphPerspective): GraphPerspective =
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
