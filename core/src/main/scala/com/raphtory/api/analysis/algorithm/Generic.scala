package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table
import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

/** Base class for writing graph algorithms that preserve views
  *
  * @define chainBody The new algorithm's `apply` method first applies this algorithm and then the `other`,
  *                   clearing all messages in-between. The `tabularise` method of the chained algorithm calls only
  *                   the `tabularise` method of `other`.
  */
trait Generic[T] extends GenericallyApplicable[T] {
  override type Out = GraphPerspective

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Main algorithm
    *
    * Default implementation returns the graph unchanged.
    * This should be overridden by subclasses to define the actual
    * algorithm steps unless the algorithm only outputs existing state or properties.
    *
    * @param graph graph to run function upon
    */
  def apply(graph: GraphPerspective): graph.Graph =
    graph.identity

  /** Chain this algorithm with another generic algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  override def ->[Q](other: Generic[Q]): Generic[Q] =
    new ChainedAlgorithm[T, Q, Generic[T], Generic[Q]](this, other) with Generic[Q] {

      override def apply(graph: GraphPerspective): graph.Graph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: GraphPerspective): Table[Q] = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] to create a new [[MultilayerProjection]]
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: MultilayerProjection[Q]): MultilayerProjection[Q] =
    new ChainedAlgorithm[T, Q, Generic[T], MultilayerProjection[Q]](this, other) with MultilayerProjection[Q] {

      override def apply(graph: GraphPerspective): graph.MultilayerGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table[Q] = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[GenericReduction]] to create a new [[GenericReduction]]
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->[Q](other: GenericReduction[Q]): GenericReduction[Q] =
    new ChainedAlgorithm[T, Q, Generic[T], GenericReduction[Q]](this, other) with GenericReduction[Q] {

      override def apply(graph: GraphPerspective): graph.ReducedGraph   =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table[Q] = second.tabularise(graph)
    }
}
