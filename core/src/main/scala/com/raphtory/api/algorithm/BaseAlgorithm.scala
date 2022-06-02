package com.raphtory.api.algorithm

import com.raphtory.api.graphview.GraphPerspective
import com.raphtory.api.graphview.MultilayerGraphPerspective
import com.raphtory.api.table.Row
import com.raphtory.api.table.Table

import scala.language.existentials

trait BaseAlgorithm extends Serializable {
  type In <: GraphPerspective
  type Out <: GraphPerspective

  def apply(graph: In): Out
  def tabularise(graph: Out): Table
  def run(graph: In): Table

  def name: String = getClass.getSimpleName

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
    *  running the other algorithm.
    *  @param graphAlgorithm next algorithm to run in the chain
    */
  def ->(graphAlgorithm: Generic): BaseAlgorithm
}

trait GenericallyApplicable extends BaseAlgorithm {
  override type In = GraphPerspective
}

abstract class ChainedAlgorithm(first: BaseAlgorithm, second: BaseAlgorithm) extends BaseAlgorithm {
  override def name: String = first.name + ":" + second.name
}
