package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Table

/** Base class for writing graph algorithms that return reduced views.
  *
  * A `GenericReduction` maps any graph view to a reduced graph view. For reduction algorithms that require the
  * input graph to be multilayer use [[MultilayerReduction]] instead.
  *
  * @define chainBody The new algorithm's `apply` method first applies this algorithm and then other,
  *                   clearing all messages inbetween. The `tabularise` method of the chained algorithm calls only
  *                   the `tabularise` method of `other`.
  */
trait GenericReduction extends GenericallyApplicable {
  override type Out = ReducedGraphPerspective

  /** Main algorithm
    *
    * Default implementation calls `graph.reducedView`. This should be overriden by subclasses to define the actual
    * algorithm steps.
    *
    * @param graph graph to run function upon
    */
  def apply(graph: GraphPerspective): graph.ReducedGraph = graph.reducedView

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: Generic): GenericReduction =
    new ChainedAlgorithm(this, other) with GenericReduction {

      override def apply(graph: GraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table  = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: GenericReduction): GenericReduction =
    new ChainedAlgorithm(this, other) with GenericReduction {

      override def apply(graph: GraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table  = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: MultilayerProjection): MultilayerProjection =
    new ChainedAlgorithm(this, other) with MultilayerProjection {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table  = second.tabularise(graph)
    }

}
