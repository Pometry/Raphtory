package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

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

  def tabularise(graph: MultilayerGraphPerspective): Table =
    graph.globalSelect(_ => Row())

  def run(graph: MultilayerGraphPerspective): Table = tabularise(apply(graph))

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  override def ->(graphAlgorithm: Generic): Multilayer =
    ChainedMultilayer(this, graphAlgorithm)

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(graphAlgorithm: GenericReduction): MultilayerReduction =
    ChainedMultilayerReduction(this, graphAlgorithm)

  /** Chain this algorithm with a [[MultilayerProjection]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(graphAlgorithm: MultilayerProjection): Multilayer =
    ChainedMultilayer2(this, graphAlgorithm)
}
