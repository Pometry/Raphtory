package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/** Base class for writing graph algorithms that return multilayer views.
  *
  * A `MultilayerProjection` maps any graph view to a multilayer graph view.
  *
  * @define chainBody The new algorithm's `apply` method first applies this algorithm and then the `other`,
  *                   clearing all messages in-between. The `tabularise` method of the chained algorithm calls only
  *                   the `tabularise` method of `other`.
  */
trait MultilayerProjection[T] extends GenericallyApplicable[T] {
  override type Out = MultilayerGraphPerspective

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
  override def ->[Q](other: Generic[Q]): MultilayerProjection[Q] =
    new ChainedAlgorithm[T, Q, MultilayerProjection[T], Generic[Q]](this, other) with MultilayerProjection[Q] {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q] = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: GenericReduction[Q]): GenericReduction[Q] =
    new ChainedAlgorithm[T, Q, MultilayerProjection[T], GenericReduction[Q]](this, other) with GenericReduction[Q] {

      override def apply(graph: GraphPerspective): graph.ReducedGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table[Q] = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[Multilayer]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: Multilayer[Q]): MultilayerProjection[Q] =
    new ChainedAlgorithm[T, Q, MultilayerProjection[T], Multilayer[Q]](this, other) with MultilayerProjection[Q] {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q] = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: MultilayerProjection[Q]): MultilayerProjection[Q] =
    new ChainedAlgorithm[T, Q, MultilayerProjection[T], MultilayerProjection[Q]](this, other)
      with MultilayerProjection[Q] {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q] = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: MultilayerReduction[Q]): GenericReduction[Q] =
    new ChainedAlgorithm[T, Q, MultilayerProjection[T], MultilayerReduction[Q]](this, other) with GenericReduction[Q] {

      override def apply(graph: GraphPerspective): graph.ReducedGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table[Q] = second.tabularise(graph)
    }

}
