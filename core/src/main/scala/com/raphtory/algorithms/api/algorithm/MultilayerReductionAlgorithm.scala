package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

trait MultilayerReductionAlgorithm extends BaseGraphAlgorithm {

  case class ChainedMultilayerReductionAlgorithm(
      first: MultilayerReductionAlgorithm,
      second: GenericAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReductionAlgorithm {

    override def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.ReducedGraph =
      second(first(graph))

    override def tabularise[G <: GraphPerspective[G]](graph: G): Table = second.tabularise(graph)
  }

  case class ChainedMultilayer2ReductionAlgorithm(
      first: MultilayerReductionAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReductionAlgorithm {

    override def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.ReducedGraph =
      second(first(graph))

    override def tabularise[G <: GraphPerspective[G]](graph: G): Table = second.tabularise(graph)
  }

  case class ChainedMultilayerAlgorithm(
      first: MultilayerReductionAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerAlgorithm {

    override def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise[G <: MultilayerGraphPerspective[G]](graph: G): Table =
      second.tabularise(graph)
  }

  def apply[G <: MultilayerGraphPerspective[G]](graph: G): graph.ReducedGraph

  def tabularise[G <: GraphPerspective[G]](graph: G): Table =
    graph.globalSelect(_ => Row())

  def run[G <: MultilayerGraphPerspective[G]](graph: G): Table =
    tabularise(apply(graph))

  override def ->(graphAlgorithm: GenericAlgorithm): MultilayerReductionAlgorithm =
    ChainedMultilayerReductionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjectionAlgorithm): MultilayerAlgorithm =
    ChainedMultilayerAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReductionAlgorithm): MultilayerReductionAlgorithm =
    ChainedMultilayer2ReductionAlgorithm(this, graphAlgorithm)

}
