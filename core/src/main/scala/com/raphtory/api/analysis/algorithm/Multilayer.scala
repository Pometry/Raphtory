package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

/** Base class for writing graph algorithms that act on multilayer views and return multilayer views.
  *
  * A `Multilayer` algorithm maps multilayer views to multilayer views and requires the
  * input graph to be multilayer.
  *
  * @define chainBody The new algorithm's `apply` method first applies this algorithm and then the `other`,
  *                   clearing all messages in-between. The `tabularise` method of the chained algorithm calls only
  *                   the `tabularise` method of `other`.
  */
trait Multilayer[T] extends BaseAlgorithm[T] {
  override type In  = MultilayerGraphPerspective
  override type Out = MultilayerGraphPerspective

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph = graph.identity

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  override def ->[Q](other: Generic[Q]): Multilayer[Q] =
    new ChainedAlgorithm[T, Q, Multilayer[T], Generic[Q]](this, other) with Multilayer[Q] {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q]         = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: GenericReduction[Q]): MultilayerReduction[Q] =
    new ChainedAlgorithm[T, Q, Multilayer[T], GenericReduction[Q]](this, other) with MultilayerReduction[Q] {

      override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table[Q]         = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: MultilayerReduction[Q]): MultilayerReduction[Q] =
    new ChainedAlgorithm[T, Q, Multilayer[T], MultilayerReduction[Q]](this, other) with MultilayerReduction[Q] {

      override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table[Q]         = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: MultilayerProjection[Q]): Multilayer[Q] =
    new ChainedAlgorithm[T, Q, Multilayer[T], MultilayerProjection[Q]](this, other) with Multilayer[Q] {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q]         = second.tabularise(graph)
    }

  /** Chain this algorithm with another Multilayer algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: Multilayer[Q]): Multilayer[Q] =
    new ChainedAlgorithm[T, Q, Multilayer[T], Multilayer[Q]](this, other) with Multilayer[Q] {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q]         = second.tabularise(graph)
    }
}
