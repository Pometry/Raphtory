package com.test.raphtory

import com.raphtory.api.analysis.algorithm.GenericReduction
import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Table
import com.raphtory.api.analysis.visitor.PropertyMergeStrategy

case class WriteValue() extends GenericReduction {

  override def apply(graph: GraphPerspective): graph.ReducedGraph =
    graph.multilayerView
      .step(vertex => vertex.setState("testing", 1))
      .reducedView(PropertyMergeStrategy.sum[Int])

  override def tabularise(graph: ReducedGraphPerspective): Table =
    graph
      .step { vertex =>
        vertex.setState("name", vertex.name())
      }
      .select("name", "testing")
}
