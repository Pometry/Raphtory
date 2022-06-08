package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

trait MultilayerProjection
        extends GenericallyApplicable
        with ConcreteAlgorithm[GraphPerspective, MultilayerGraphPerspective] {

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  override def apply(graph: GraphPerspective): graph.MultilayerGraph =
    graph.multilayerView

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  override def ->(other: Generic): MultilayerProjection =
    new ChainedAlgorithm(this, other) with MultilayerProjection {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table  = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(graphAlgorithm: GenericReduction): GenericReduction =
    new ChainedAlgorithm(this, graphAlgorithm) with GenericReduction {

      override def apply(graph: GraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table  = second.tabularise(graph)
    }

  def ->(other: Multilayer): MultilayerProjection =
    new ChainedAlgorithm(this, other) with MultilayerProjection {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table  = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(graphAlgorithm: MultilayerProjection): MultilayerProjection =
    new ChainedAlgorithm(this, graphAlgorithm) with MultilayerProjection {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table  = second.tabularise(graph)
    }

  def ->(other: MultilayerReduction): GenericReduction =
    new ChainedAlgorithm(this, other) with GenericReduction {

      override def apply(graph: GraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table  = second.tabularise(graph)
    }

}
