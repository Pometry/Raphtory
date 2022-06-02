package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.ReducedGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

trait GenericReduction extends GenericallyApplicable {
  override type Out = ReducedGraphPerspective

  case class ChainedGenericReduction(
      first: GenericReduction,
      second: Generic
  ) extends ChainedAlgorithm(first, second)
          with GenericReduction {

    override def apply(graph: GraphPerspective): graph.ReducedGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: ReducedGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class Chained2GenericReduction(
      first: GenericReduction,
      second: GenericReduction
  ) extends ChainedAlgorithm(first, second)
          with GenericReduction {

    override def apply(graph: GraphPerspective): graph.ReducedGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: ReducedGraphPerspective): Table = second.tabularise(graph)
  }

  case class ChainedMultilayerProjection(
      first: GenericReduction,
      second: MultilayerProjection
  ) extends ChainedAlgorithm(first, second)
          with MultilayerProjection {

    override def apply(graph: GraphPerspective): graph.MultilayerGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: GraphPerspective): graph.ReducedGraph = graph.reducedView

  /** Return tabularised results (default implementation returns empty table)
    *
    * @param graph graph to run function upon
    */
  def tabularise(graph: ReducedGraphPerspective): Table =
    graph.globalSelect(_ => Row())

  override def run(graph: GraphPerspective): Table = tabularise(apply(graph))

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
    * running the other algorithm.
    *
    * @param graphAlgorithm next algorithm to run in the chain
    */
  override def ->(graphAlgorithm: Generic): GenericReduction =
    ChainedGenericReduction(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReduction): GenericReduction =
    Chained2GenericReduction(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjection): MultilayerProjection =
    ChainedMultilayerProjection(this, graphAlgorithm)

}
