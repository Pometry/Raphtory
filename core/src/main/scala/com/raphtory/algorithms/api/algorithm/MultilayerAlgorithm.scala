package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

trait MultilayerAlgorithm extends BaseGraphAlgorithm {

  case class ChainedMultilayerAlgorithm(
      first: MultilayerAlgorithm,
      second: GenericAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerAlgorithm {

    override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph = {
      val t = first(graph)
      second(first(graph))
    }

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayer2Algorithm(
      first: MultilayerAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerAlgorithm {

    override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayerReductionAlgorithm(
      first: MultilayerAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReductionAlgorithm {

    override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
      second(first(graph))

    override def tabularise(graph: GraphPerspective): Table = second.tabularise(graph)
  }

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph

  /** Return tabularised results (default implementation returns empty table)
    *
    * @param graph graph to run function upon
    */
  def tabularise(graph: MultilayerGraphPerspective): Table =
    graph.globalSelect(_ => Row())

  def run(graph: MultilayerGraphPerspective): Table = tabularise(apply(graph))

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
    * running the other algorithm.
    *
    * @param graphAlgorithm next algorithm to run in the chain
    */
  override def ->(graphAlgorithm: GenericAlgorithm): MultilayerAlgorithm =
    ChainedMultilayerAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReductionAlgorithm): MultilayerReductionAlgorithm =
    ChainedMultilayerReductionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjectionAlgorithm): MultilayerAlgorithm =
    ChainedMultilayer2Algorithm(this, graphAlgorithm)
}
