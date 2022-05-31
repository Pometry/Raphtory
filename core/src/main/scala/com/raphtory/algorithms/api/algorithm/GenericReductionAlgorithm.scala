package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

trait GenericReductionAlgorithm extends GenericallyApplicableAlgorithm {

  case class ChainedGenericReductionAlgorithm(
      first: GenericReductionAlgorithm,
      second: GenericAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with GenericReductionAlgorithm {

    override def apply(graph: GraphPerspective): graph.ReducedGraph =
      second(first(graph))

    override def tabularise(graph: GraphPerspective): Table =
      second.tabularise(graph)
  }

  case class Chained2GenericReductionAlgorithm(
      first: GenericReductionAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with GenericReductionAlgorithm {

    override def apply(graph: GraphPerspective): graph.ReducedGraph =
      second(first(graph))

    override def tabularise(graph: GraphPerspective): Table = second.tabularise(graph)
  }

  case class ChainedMultilayerProjectionAlgorithm(
      first: GenericReductionAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerProjectionAlgorithm {

    override def apply(graph: GraphPerspective): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: GraphPerspective): graph.ReducedGraph

  /** Return tabularised results (default implementation returns empty table)
    *
    * @param graph graph to run function upon
    */
  def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(_ => Row())

  override def run(graph: GraphPerspective): Table = tabularise(apply(graph))

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
    * running the other algorithm.
    *
    * @param graphAlgorithm next algorithm to run in the chain
    */
  override def ->(graphAlgorithm: GenericAlgorithm): GenericReductionAlgorithm =
    ChainedGenericReductionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReductionAlgorithm): GenericReductionAlgorithm =
    Chained2GenericReductionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjectionAlgorithm): MultilayerProjectionAlgorithm =
    ChainedMultilayerProjectionAlgorithm(this, graphAlgorithm)

}
