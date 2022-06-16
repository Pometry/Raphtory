package com.raphtory.algorithms

import com.raphtory.BaseCorrectnessTest
import com.raphtory.TestQuery
import com.raphtory.algorithms.generic.EdgeList
import com.raphtory.algorithms.generic.NodeList
import com.raphtory.algorithms.temporal.TemporalEdgeList
import com.raphtory.algorithms.temporal.TemporalNodeList
import com.raphtory.algorithms.temporal.views.MultilayerView
import com.raphtory.api.analysis.algorithm.Generic
import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy
import com.raphtory.api.input.Spout
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

class MultilayerViewTest extends BaseCorrectnessTest(startGraph = true) {
  val edges = Seq("1,2,1", "2,1,2")

  override def setSpout(): Spout[String] = SequenceSpout(edges: _*)

  test("test multilayer view") {
    assert(
            correctnessTest(
                    TestQuery(MultilayerView() -> EdgeList(), 2),
                    Seq("2,1_1,2_1", "2,2_2,1_2")
            )
    )
  }

  test("test temporal node list") {
    assert(
            correctnessTest(
                    TestQuery(TemporalNodeList(), 2),
                    Seq("2,1,1", "2,1,2", "2,2,1", "2,2,2")
            )
    )
  }

  test("test temporal edge list") {
    assert(
            correctnessTest(TestQuery(TemporalEdgeList(), 2), Seq("2,1,2,1", "2,2,1,2"))
    )
  }

  test("test property merging") {
    assert(
            correctnessTest(
                    TestQuery(WriteValue() -> NodeList("testing"), 2),
                    Seq("2,1,2", "2,2,2")
            )
    )
  }

}
