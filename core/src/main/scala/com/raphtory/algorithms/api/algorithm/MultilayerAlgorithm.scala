package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.AbstractGraph
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

    override def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise[G <: MultilayerGraphPerspective[G]](graph: G): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayer2Algorithm(
      first: MultilayerAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerAlgorithm {

    override def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise[G <: MultilayerGraphPerspective[G]](graph: G): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayerReductionAlgorithm(
      first: MultilayerAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReductionAlgorithm {

    override def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.ReducedGraph =
      second(first(graph))

    override def tabularise[G <: GraphPerspective[G]](graph: G): Table = second.tabularise(graph)
  }

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.MultilayerGraph

  /** Return tabularised results (default implementation returns empty table)
    *
    * @param graph graph to run function upon
    */
  def tabularise[G <: MultilayerGraphPerspective[G]](graph: G): Table =
    graph.globalSelect(_ => Row())

  def run[G <: MultilayerGraphPerspective[G]](graph: G): Table = tabularise(apply(graph))

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
