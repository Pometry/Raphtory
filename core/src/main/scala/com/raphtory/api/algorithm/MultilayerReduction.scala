package com.raphtory.api.algorithm

import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.graphview.MultilayerGraphPerspective
import com.raphtory.api.graphview.ReducedGraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table

trait MultilayerReduction extends BaseAlgorithm {
  override type In  = MultilayerGraphPerspective
  override type Out = ReducedGraphPerspective

  case class ChainedMultilayerReduction(
      first: MultilayerReduction,
      second: Generic
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReduction {

    override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: ReducedGraphPerspective): Table = second.tabularise(graph)
  }

  case class ChainedMultilayer2Reduction(
      first: MultilayerReduction,
      second: GenericReduction
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReduction {

    override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: ReducedGraphPerspective): Table = second.tabularise(graph)
  }

  case class ChainedMultilayer(
      first: MultilayerReduction,
      second: MultilayerProjection
  ) extends ChainedAlgorithm(first, second)
          with Multilayer {

    override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph = graph.reducedView

  def tabularise(graph: ReducedGraphPerspective): Table =
    graph.globalSelect(_ => Row())

  def run(graph: MultilayerGraphPerspective): Table =
    tabularise(apply(graph))

  override def ->(graphAlgorithm: Generic): MultilayerReduction =
    ChainedMultilayerReduction(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjection): Multilayer =
    ChainedMultilayer(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReduction): MultilayerReduction =
    ChainedMultilayer2Reduction(this, graphAlgorithm)

}
