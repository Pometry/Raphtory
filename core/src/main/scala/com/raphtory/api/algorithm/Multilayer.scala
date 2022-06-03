package com.raphtory.api.algorithm

import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.graphview.MultilayerGraphPerspective
import com.raphtory.api.graphview.ReducedGraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table

trait Multilayer extends BaseAlgorithm {
  override type In  = MultilayerGraphPerspective
  override type Out = MultilayerGraphPerspective

  case class ChainedMultilayer(
      first: Multilayer,
      second: Generic
  ) extends ChainedAlgorithm(first, second)
          with Multilayer {

    override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayer2(
      first: Multilayer,
      second: MultilayerProjection
  ) extends ChainedAlgorithm(first, second)
          with Multilayer {

    override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: MultilayerGraphPerspective): Table =
      second.tabularise(graph)
  }

  case class ChainedMultilayerReduction(
      first: Multilayer,
      second: GenericReduction
  ) extends ChainedAlgorithm(first, second)
          with MultilayerReduction {

    override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
      second(first(graph).clearMessages())

    override def tabularise(graph: ReducedGraphPerspective): Table = second.tabularise(graph)
  }

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph = graph.identity

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
  override def ->(graphAlgorithm: Generic): Multilayer =
    ChainedMultilayer(this, graphAlgorithm)

  def ->(graphAlgorithm: GenericReduction): MultilayerReduction =
    ChainedMultilayerReduction(this, graphAlgorithm)

  def ->(graphAlgorithm: MultilayerProjection): Multilayer =
    ChainedMultilayer2(this, graphAlgorithm)
}
