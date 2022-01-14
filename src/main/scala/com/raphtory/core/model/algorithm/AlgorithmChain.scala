package com.raphtory.core.model.algorithm

class AlgorithmChain(algorithms: Seq[GraphAlgorithm]) extends GraphAlgorithm {

  override def algorithm(graphPerspective: GraphPerspective): GraphPerspective = {
    var gp = graphPerspective
    if(algorithms.nonEmpty) {
      for(algorithm <- algorithms)
        gp = algorithm.algorithm(gp)
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

}

object AlgorithmChain {
  def apply(algorithms: GraphAlgorithm*) = new AlgorithmChain(algorithms)
}
