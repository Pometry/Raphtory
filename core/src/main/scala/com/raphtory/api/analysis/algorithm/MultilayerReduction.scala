package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Table

/** Base class for writing graph algorithms that map multilayer views to reduced views.
  *
  * A `MultilayerReduction` maps a multilayer view to a reduced graph view and requires the
  * input graph to be multilaye.
  *
  * @define chainBody The new algorithm's `apply` method first applies this algorithm and then other,
  *                   clearing all messages inbetween. The `tabularise` method of the chained algorithm calls only
  *                   the `tabularise` method of `other`.
  */
trait MultilayerReduction extends BaseAlgorithm {
  override type In  = MultilayerGraphPerspective
  override type Out = ReducedGraphPerspective

  def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph = graph.reducedView

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  override def ->(other: Generic): MultilayerReduction =
    new ChainedAlgorithm(this, other) with MultilayerReduction

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: GenericReduction): MultilayerReduction =
    new ChainedAlgorithm(this, other) with MultilayerReduction {

      override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table            = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: MultilayerProjection): Multilayer =
    new ChainedAlgorithm(this, other) with Multilayer {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table            = second.tabularise(graph)
    }

}
