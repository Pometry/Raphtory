package com.raphtory.core.algorithm

import scala.Seq

class Chain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm {

  override def apply(graphPerspective: GraphPerspective): GraphPerspective = {
    var gp = graphPerspective
    if (algorithms.nonEmpty)
      for (algorithm <- algorithms)
        gp = algorithm.apply(gp).asInstanceOf[GenericGraphPerspective].clearMessages()
    gp
  }

  override def tabularise(graph: GraphPerspective): Table =
    if (algorithms.nonEmpty)
      algorithms.last.tabularise(graph)
    else
      super.tabularise(graph)

  override def ->(graphAlgorithm: GraphAlgorithm): Chain =
    Chain(algorithms.toList ++ List(graphAlgorithm))

}

object Chain {
  def apply(algorithms: GraphAlgorithm*)      = new Chain(algorithms)
  def apply(algorithms: List[GraphAlgorithm]) = new Chain(algorithms)
}
