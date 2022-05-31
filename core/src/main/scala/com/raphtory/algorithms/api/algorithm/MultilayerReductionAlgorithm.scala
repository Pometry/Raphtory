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

    override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
      second(first(graph))

    override def tabularise(graph: GraphPerspective): Table = second.tabularise(graph)
  }

  case class ChainedMultilayer2ReductionAlgorithm(
      first: MultilayerReductionAlgorithm,
      second: GenericReductionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReductionAlgorithm {

    override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
      second(first(graph))

    override def tabularise(graph: GraphPerspective): Table = second.tabularise(graph)
  }

  case class ChainedMultilayerAlgorithm(
      first: MultilayerReductionAlgorithm,
      second: MultilayerProjectionAlgorithm
  ) extends ChainedAlgorithm(first, second)
          with MultilayerAlgorithm {

    override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
      second(first(graph))

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph

  def tabularise(graph: GraphPerspective): Table =
    graph.globalSelect(_ => Row())

  def run(graph: MultilayerGraphPerspective): Table =
    tabularise(apply(graph))

  override def ->(graphAlgorithm: GenericAlgorithm): MultilayerReductionAlgorithm =
    ChainedMultilayerReductionAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjectionAlgorithm): MultilayerAlgorithm =
    ChainedMultilayerAlgorithm(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReductionAlgorithm): MultilayerReductionAlgorithm =
    ChainedMultilayer2ReductionAlgorithm(this, graphAlgorithm)

}
