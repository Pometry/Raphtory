package com.raphtory.algorithms.api.algorithm

import com.raphtory.algorithms.api.AbstractGraph
import com.raphtory.algorithms.api.GraphPerspective
import com.raphtory.algorithms.api.MultilayerGraphPerspective
import com.raphtory.algorithms.api.Row
import com.raphtory.algorithms.api.Table

import scala.language.existentials

trait BaseGraphAlgorithm extends Serializable {

  def name: String = getClass.getSimpleName

  /** Create a new algorithm [](com.raphtory.algorithms.api.Chain) which runs this algorithm first before
    *  running the other algorithm.
    *  @param graphAlgorithm next algorithm to run in the chain
    */
  def ->(graphAlgorithm: GenericAlgorithm): BaseGraphAlgorithm
}

trait GenericallyApplicableAlgorithm extends BaseGraphAlgorithm {
  def apply[G <: GraphPerspective[G]](graph: G): AbstractGraph

  def tabularise[G <: MultilayerGraphPerspective[G]](graph: G): Table

  def run[G <: GraphPerspective[G]](graph: G): Table
}

abstract class ChainedAlgorithm(first: BaseGraphAlgorithm, second: BaseGraphAlgorithm)
        extends BaseGraphAlgorithm {
  override def name: String = first.name + "->" + second.name
}
