package com.raphtory.api.analysis.algorithm

import com.raphtory.api.analysis.graphview.GraphPerspective
import com.raphtory.api.analysis.graphview.MultilayerGraphPerspective
import com.raphtory.api.analysis.graphview.ReducedGraphPerspective
import com.raphtory.api.analysis.table.Row
import com.raphtory.api.analysis.table.Table

trait Multilayer extends ConcreteAlgorithm[MultilayerGraphPerspective, MultilayerGraphPerspective] {

  /** Default implementation returns the graph unchanged
    *
    * @param graph graph to run function upon
    */
  def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph = graph.identity

  /** Chain this algorithm with a [[Generic]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  override def ->(other: Generic): Multilayer =
    new ChainedAlgorithm(this, other) with Multilayer {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table            = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[GenericReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: GenericReduction): MultilayerReduction =
    new ChainedAlgorithm(this, other) with MultilayerReduction {

      override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table            = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerReduction]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: MultilayerReduction): MultilayerReduction =
    new ChainedAlgorithm(this, other) with MultilayerReduction {

      override def apply(graph: MultilayerGraphPerspective): graph.ReducedGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: ReducedGraphPerspective): Table            = second.tabularise(graph)
    }

  /** Chain this algorithm with a [[MultilayerProjection]] algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: MultilayerProjection): Multilayer =
    new ChainedAlgorithm(this, other) with Multilayer {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table            = second.tabularise(graph)
    }

  /** Chain this algorithm with another Multilayer algorithm
    *
    * $chainBody
    * @param other Algorithm to apply after this one
    */
  def ->(other: Multilayer): Multilayer =
    new ChainedAlgorithm(this, other) with Multilayer {

      override def apply(graph: MultilayerGraphPerspective): graph.MultilayerGraph =
        second(first(graph).clearMessages())
      override def tabularise(graph: MultilayerGraphPerspective): Table            = second.tabularise(graph)
    }
}
