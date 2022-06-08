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
  * @define chainBody The new algorithm's `apply` method first applies this algorithm and then other,
  *                   clearing all messages inbetween. The `tabularise` method of the chained algorithm calls only
  *                   the `tabularise` method of `other`.
  */
trait Generic extends GenericallyApplicable {
  override type Out = GraphPerspective

  /** Logger instance for writing out log messages */
  val logger: Logger = Logger(LoggerFactory.getLogger(this.getClass))

  /** Main algorithm
    *
    * Default implementation returns the graph unchanged.
    * This should be overriden by subclasses to define the actual
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
  override def ->(other: Generic): Generic =
    new ChainedAlgorithm(this, other) with Generic {

      override def apply(graph: GraphPerspective): graph.Graph =
        second(first(graph).clearMessages())
      override def tabularise(graph: GraphPerspective): Table  = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] to create a new [[MultilayerProjection]]
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

  /** Chain this algorithm with a [[GenericReduction]] to create a new [[GenericReduction]]
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
}
