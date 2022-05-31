package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

trait MultilayerProjectionAlgorithm extends GenericallyApplicableAlgorithm {

  case class ChainedMultilayerProjectionAlgorithm(
      first: MultilayerProjectionAlgorithm,
      second: GenericAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerProjectionAlgorithm {

    override def apply(graph: GraphPerspective): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayerProjection2Algorithm(
      first: MultilayerProjectionAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerProjectionAlgorithm {

    override def apply(graph: GraphPerspective): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedReductionAlgorithm(
      first: MultilayerProjectionAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with GenericReductionAlgorithm {

    override def apply(graph: GraphPerspective): graph.ReducedGraph =
      second(first(graph))

    override def tabularise(graph: GraphPerspective): Table = second.tabularise(graph)
  }

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  override def apply(graph: GraphPerspective): graph.MultilayerGraph

  /** Return tabularised results (default implementation returns empty table)
    *
    * @param graph graph to run function upon
    */
  def tabularise(graph: MultilayerGraphPerspective): Table =
    graph.globalSelect(_ => Row())

  override def run(graph: GraphPerspective): Table =
    tabularise(apply(graph))

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
    * running the other algorithm.
    *
    * @param graphAlgorithm next algorithm to run in the chain
    */
  override def ->(graphAlgorithm: GenericAlgorithm): MultilayerProjectionAlgorithm =
    ChainedMultilayerProjectionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjectionAlgorithm): MultilayerProjectionAlgorithm =
    ChainedMultilayerProjection2Algorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReductionAlgorithm): GenericReductionAlgorithm =
    ChainedReductionAlgorithm(this, graphAlgorithm)
}
