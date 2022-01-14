package com.raphtory.core.model.algorithm

import scala.Seq

class Chain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm {

  override def apply(graphPerspective: GraphPerspective): GraphPerspective = {
    var gp = graphPerspective
    if(algorithms.nonEmpty) {
      for(algorithm <- algorithms)
        gp = algorithm.apply(gp)
    }
    gp
  }

  override def tabularise(graph: GraphPerspective): Table = {
    if(algorithms.nonEmpty)
      algorithms.last.tabularise(graph)
    else
      super.tabularise(graph)
  }

  override def write(table: Table): Unit = {
    if(algorithms.nonEmpty)
      algorithms.last.write(table)
    else
      super.write(table)
  }

  override def ->(graphAlgorithm: GraphAlgorithm):Chain = {
    Chain(algorithms.toList ++ List(graphAlgorithm))
  }

}

object Chain {
  def apply(algorithms: GraphAlgorithm*) = new Chain(algorithms)
  def apply(algorithms: List[GraphAlgorithm]) = new Chain(algorithms)
}
